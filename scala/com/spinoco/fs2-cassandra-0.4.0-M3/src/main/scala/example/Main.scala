package example

import cats.effect._
import cats.implicits._
import com.datastax.driver.core.{Cluster, Row}
import fs2.Stream
import spinoco.fs2.cassandra.{CassandraCluster, CassandraSession, Query}

object Main extends IOApp {
  final val server = "[server url]"

  override def run(args: List[String]): IO[ExitCode] = {

    val config = Cluster.builder().addContactPoints(server)

    val tableNames = List(
      "story.test1",
      "story.mentioned_activities",
      "story.user_followee_recent_relation",
      "story.feed_lock_in_users_v2",
      "story.test2",
      "story.friend_recently_selected",
      "story.birthday_update_feed_activate",
      "story.recent_friend_activities",
      "story.friend_recently_selected_test",
      "story.crizin",
      "story.album_action_counts",
      "story.test",
      "story.specific_friends",
      "story.friend_recent_relation",
      "story.feature_guide_feed_deliveries",
      "story.birthday_friends",
      "story.name_change_history",
      "story.storyteller_follow_feed_history",
      "story.channel_recommend_counter",
      "story.agreement_history",
      "story.mentioned_activity_types",
      "story.crizin1",
      "story.feed_lock_in_remove_channels",
      "story.profile_view_counter",
      "story.part_of_friend_permission_activities"
    )

    def countKeys(session: CassandraSession[IO], tableName: String): Stream[IO, (String, Long)] =
      session.queryCql(s"select count(*) from $tableName").map(x => (tableName, x.getLong(0)))

    /*
    val stream =
      CassandraCluster[IO](config, None)
        .flatMap(cluster => cluster.session)
        .flatMap { session =>
          Stream
            .emits[IO, String](tableNames)
            .flatMap(tableName => countKeys(session, tableName))
        }
        .evalMap(row => IO(println(row)))
     */
    val stream =
      CassandraCluster[IO](config, None)
        .flatMap(cluster => cluster.session)
        .flatMap { session =>
          session.queryCql(s"select * from story.part_of_friend_permission_activities")
        }
        .evalMap(row => IO(println(row)))

    stream.compile.drain.as(ExitCode.Success)
  }
}
