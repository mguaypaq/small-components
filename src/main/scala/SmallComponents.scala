package ca.pointedset.graph

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Compute the vertex sets of small connected components of a graph.
  *
  * The input graph is given as an RDD of edges, that is, `(Vertex, Vertex)` pairs.
  * The edges are automatically considered in both directions, and self-loops are ignored.
  * If a vertex is not connected to any other vertex, it is dropped.
  *
  * The output is given as an RDD of sorted `List[Vertex]` objects of length between 2 and `maxSize`, inclusive.
  * They are the sets of vertices of the small connected components in the input graph.
  *
  * The type `Vertex` of the vertices should be ordered.
  * For performance reasons, the vertex hashes should be roughly uniformly distributed,
  * and the vertices should have a compact serialization.
  *
  * The `parallelism` parameter controls in how many chunks the set of vertices is split up during processing.
  * If it's too low, some of the chunks won't fit into a single executor's memory,
  * causing them to spill to disk, and to take orders of magnitude more processing time.
  * Meanwhile all the other executors will be sitting idle.
  * If it's too high, the overhead of serialization, etc. will become significant.
  * Still, it's probably much better to err on the side of too high rather than too low.
  *
  * The algorithm implemented here is essentially the ''Alternating'' algorithm described in the paper:
  *
  * Connected Components in MapReduce and Beyond
  * Raimondas Kiveris, Silvio Lattanzi, Vahab Mirrokni, Vibhor Rastogi, Sergei Vassilvitskii
  * SOCC 2014
  * web: [[https://doi.org/10.1145/2670979.2670997]]
  *
  * The main difference is that we detect when a vertex is essentially no longer active during processing,
  * (that is, when it has a single neighbour, and that vertex comes earlier in the vertex ordering),
  * in which case it gets merged into a set of inactive vertices attached to its neighbour.
  * Since large sets of inactive vertices will be discarded in the output, they are simply replaced by a
  * special value which indicates a large set, rather than keeping track of all their elements.
  * This also means that the number of active vertices grows much smaller from round to round of processing.
  *
  * Another difference is that we start the algorithm with two consecutive steps of the small-star operation.
  * This is because our graphs typically have a majority of isolated edges
  * (that is, connected components with just two vertices connected by a single edge).
  * By running the small-star operation twice, we collect these very small connected components right away,
  * which drastically reduces the number of active vertices.
  */
class SmallComponents[Vertex <: Ordered[Vertex]: ClassTag]
                     (maxSize: Int, parallelism: Int) extends Serializable {
  @transient lazy val log: Logger = LogManager.getLogger("SmallComponents")

  /* We want to use the same partitioner every time we do a `groupByKey` where the keys are vertices */
  val partitioner = new HashPartitioner(parallelism)

  type Edge = (Vertex, Vertex)
  type Component = List[Vertex]

  /* A `BoundedSet` is a set of inactive vertices attached to a still-active vertex.
   *
   * A small set of vertices (up to `maxSize`, possibly empty) is represented as `Some(Set(vertices))`.
   * A large set of vertices (more than `maxSize`) is represented as `None`.
   *
   * See the methods `boundedAdd` and `boundedMerge`.
   */
  type BoundedSet = Option[Set[Vertex]]

  /* A `Message` is a piece of information sent to an active vertex.
   *
   * A set of inactive vertices attached to an active vertex is represented as `Left(boundedSet)`.
   * An active neighbour is represented as `Right(vertex)`.
   *
   * The methods `largeStar` and `smallStar` each take an `Inbox` of messages for an active vertex
   * and generate an `Outbox` of messages to other active vertices.
   */
  type Message = Either[BoundedSet, Vertex]
  type Inbox = (Vertex, Iterable[Message])
  type Outbox = List[(Vertex, Message)]

  def run(edges: RDD[Edge]): RDD[Component] = {
    val sc: SparkContext = edges.sparkContext
    /* Accumulator for the small connected components computed so far. */
    val components = Seq.newBuilder[RDD[Component]]
    /* The inboxes of messages for all still-active vertices.
     * The `doSmallStar` and `doLargeStar` methods are responsible for unpersisting old versions of this.
     */
    var inboxes: RDD[Inbox] = edges
      .flatMap(initialMessages)
      .groupByKey(partitioner)
      .persist()

    def doSmallStarAndUnzip(): Unit = {
      val (newComponents, newInboxes) = doSmallStar(inboxes)
      components += newComponents
      inboxes = newInboxes
    }

    /* Start with two small-star operations to get isolated edges out of the way ASAP. */
    doSmallStarAndUnzip()
    doSmallStarAndUnzip()

    /* Then alternate large-star and small-star operations until convergence. */
    while (!inboxes.isEmpty()) {
      inboxes = doLargeStar(inboxes)
      doSmallStarAndUnzip()
    }
    inboxes.unpersist()

    sc.union(components.result())
  }

  /** For a given input edge, if it's not a self-loop, tell each of its vertices that it has an active neighbour. */
  def initialMessages(edge: Edge): Outbox = {
    val (u, v) = edge
    if (u != v) List(
      (u, Right(v)),
      (v, Right(u))
    ) else List()
  }

  /** Add a vertex to a bounded set, and replace it with `None` if that makes the set too big. */
  def boundedAdd(boundedSet: BoundedSet, vertex: Vertex): BoundedSet =
    for {
      set <- boundedSet
      result = set + vertex
      if result.size <= maxSize
    } yield result

  /** Merge two bounded sets together, and replace them with `None` if the union is too big. */
  def boundedMerge(boundedSet: BoundedSet, otherBoundedSet: BoundedSet): BoundedSet =
    for {
      set <- boundedSet
      otherSet <- otherBoundedSet
      result = set ++ otherSet
      if result.size <= maxSize
    } yield result

  /** A large-star operation should reduce the length of the longest decreasing path in the graph by half.
    *
    * However, it can't detect completed components or make vertices inactive.
    */
  def doLargeStar(inboxes: RDD[Inbox]): RDD[Inbox] = {
    val newInboxes = inboxes
      .flatMap(largeStar)
      .groupByKey(partitioner)
      .persist()
    /* The `count()` is necessary to materialize the new inboxes before unpersisting the old ones. */
    val count = newInboxes.count()
    log.info(s"Active vertices after large-star: $count")
    inboxes.unpersist()
    newInboxes
  }

  /** Process the message inbox of a single active vertex (the ''center'') in a large-star step.
    *
    * In a large-star step, an active vertex owns the edges going to its _bigger_ neighbours
    * (according to the ordering on vertices), so it is responsible for emitting the replacement
    * edges that they turn into.
    *
    * Specifically, an old edge `(centerVertex, bigger)` should turn into a new edge `(min, bigger)`,
    * where `min` is the smallest vertex that `center` sees (either a current neighbour or itself).
    *
    * Also, if there is a set of inactive vertices tied to `center`, then they are transferred to `min`.
    */
  def largeStar(inbox: Inbox): Outbox = {
    val (center, messages) = inbox

    /* These three values are computed in a single pass over the inbox. */
    var min: Vertex = center
    var larger: List[Vertex] = List()
    var inactive: BoundedSet = Some(Set())

    messages.foreach {
      case Left(otherInactive) =>
        inactive = boundedMerge(inactive, otherInactive)
      case Right(vertex) =>
        if (vertex < min) {
          min = vertex
        } else if (vertex > center) {
          larger = vertex :: larger
        }
    }

    /* Note: an empty bounded set is represented as `Some(Set())`, not as `None`. */
    val inactiveEmpty: Boolean = inactive.exists(_.isEmpty)
    val acc: Outbox = if (inactiveEmpty) Nil else (min, Left(inactive)) :: Nil
    /* Both `min` and `vertex` need to know about the new edge between them.
     * We always have `min <= center < vertex`, so `min != vertex`.
     */
    val outbox: Outbox = larger.foldLeft(acc) { (acc, vertex) =>
      (min, Right(vertex)) :: (vertex, Right(min)) :: acc
    }
    outbox
  }

  /** A small-star operation should make any active vertex which doesn't have bigger neighbours, inactive.
    *
    * It also detects completed components (that is, an active vertex which doesn't have active neighbours
    * anymore, only a set of inactive vertices attached to it). They are returned as a separate RDD.
    */
  def doSmallStar(inboxes: RDD[Inbox]): (RDD[Component], RDD[Inbox]) = {
    /* We want to generate two output RDDs in a single pass through the input RDD,
     * so `smallStar` returns an RDD of `Either`, which we separate out afterwards.
     */
    val both = inboxes
      .map(smallStar)
      .persist()
    /* The `count()`s are necessary to materialize the new RDDs before unpersisting their input RDDs. */
    both.count()
    inboxes.unpersist()
    val newComponents: RDD[Component] = (for (Left(component) <- both) yield component).persist()
    val newComponentsCount = newComponents.count()
    log.info(s"New components after small-star: $newComponentsCount")
    val newInboxes = both
      .flatMap({ case Right(outbox) => outbox; case _ => List() })
      .groupByKey(partitioner)
      .persist()
    val newInboxesCount = newInboxes.count()
    log.info(s"Active vertices after small-star: $newInboxesCount")
    both.unpersist()
    (newComponents, newInboxes)
  }

  /** Process the message inbox of a single active vertex in a small-star step.
    *
    * In a small-star step, an active vertex owns the edges going to its _smaller_ neighbours
    * (according to the ordering on vertices), so it is responsible for emitting the replacement
    * edges that they turn into.
    *
    * Specifically, an old edge `(center, smaller)` should turn into a new edge `(min, smaller)`,
    * where `min` is the smallest neighbour of that `center` sees (either a current neighbour or itself).
    *
    * Also, if there is a set of inactive vertices tied to `center`, then they are transferred to `min`.
    *
    * There are a few exceptions that complicate the logic, however:
    *
    * 1. The edge `(center, smaller)` when `smaller == min` shouldn't into the self-loop
    *    `(min, min)`; it should stay the same.
    *
    * 2. If `center != min` and it has no bigger active neighbours, then `center`
    *    can be turned into an inactive vertex, and added to the set of inactive
    *    vertices transferred to `min`.
    *
    * 3. If `center` has no smaller active neighbours, then `min == center`.
    *    (This is not really an exception, just something to keep in mind.)
    *
    * 4. If `center` has no active neighbours at all, then it forms a connected component
    *    together with its attached set of inactive vertices.
    */
  def smallStar(inbox: Inbox): Either[Component, Outbox] = {
    val (center, messages) = inbox

    /* These three values are computed in a single pass over the inbox.
     * Also, we maintain the invariant that the first element of the list
     * `smaller` (if any) is the smallest active neighbour seen so far.
     */
    var smaller: List[Vertex] = List()
    var seenBigger: Boolean = false
    var inactive: BoundedSet = Some(Set())

    messages.foreach {
      case Left(otherInactive) =>
        inactive = boundedMerge(inactive, otherInactive)
      case Right(vertex) =>
        if (vertex < center) {
          /* Add `vertex` to the list of smaller neighbours, maintaining the invariant stated above. */
          smaller = smaller match {
            case minSoFar :: tail if minSoFar < vertex =>
              minSoFar :: vertex :: tail
            case _ =>
              vertex :: smaller
          }
        } else if (vertex > center) {
          seenBigger = true
        }
    }

    /* What to do next depends on two factors:
     * - whether `center` has any smaller neighbours, and
     * - whether `center` has any bigger neighbours.
     */
    val min: Vertex = (smaller, seenBigger) match {
      case (minSoFar :: tail, true) =>
        /* This is exceptional case 1. */
        smaller = center :: tail
        minSoFar
      case (minSoFar :: tail, false) =>
        /* This is exceptional case 2. */
        smaller = tail
        inactive = boundedAdd(inactive, center)
        minSoFar
      case (Nil, true) =>
        /* This is exceptional case 3. */
        center
      case (Nil, false) =>
        /* This is exceptional case 4. */
        val component = boundedAdd(inactive, center)
        component match {
          case Some(set) =>
            /* We have identified an actual small connected component. */
            return Left(set.toList.sorted)
          case None =>
            /* We have identified a large connected component, so ignore it. */
            return Right(List())
        }
    }

    /* Note: an empty bounded set is represented as `Some(Set())`, not as `None`. */
    val inactiveEmpty: Boolean = inactive.exists(_.isEmpty)
    val acc: Outbox = if (inactiveEmpty) Nil else (min, Left(inactive)) :: Nil
    val outbox: Outbox = smaller.foldLeft(acc) { (acc, vertex) =>
      if (min != vertex)
        /* Both `min` and `vertex` need to know about the new edge between them. */
        (min, Right(vertex)) :: (vertex, Right(min)) :: acc
      else
        acc
    }
    Right(outbox)
  }
}
