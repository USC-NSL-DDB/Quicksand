namespace cpp social_network
namespace py social_network
namespace lua social_network

struct User {
    1: i64 user_id;
    2: string first_name;
    3: string last_name;
    4: string username;
    5: string password_hashed;
    6: string salt;
}

enum ErrorCode {
  SE_CONNPOOL_TIMEOUT,
  SE_THRIFT_CONN_ERROR,
  SE_UNAUTHORIZED,
  SE_MEMCACHED_ERROR,
  SE_MONGODB_ERROR,
  SE_REDIS_ERROR,
  SE_THRIFT_HANDLER_ERROR,
  SE_RABBITMQ_CONN_ERROR
}

exception ServiceException {
    1: ErrorCode errorCode;
    2: string message;
}

enum PostType {
  POST,
  REPOST,
  REPLY,
  DM
}

struct Media {
  1: i64 media_id;
  2: string media_type;
}

struct Url {
  1: string shortened_url;
  2: string expanded_url;
}

struct UserMention {
  1: i64 user_id;
  2: string username;
}

struct Creator {
  1: i64 user_id;
  2: string username;
}

struct TextServiceReturn {
 1: string text;
 2: list<UserMention> user_mentions;
 3: list<Url> urls;
}

struct Post {
  1: i64 post_id;
  2: Creator creator;
  3: i64 req_id;
  4: string text;
  5: list<UserMention> user_mentions;
  6: list<Media> media;
  7: list<Url> urls;
  8: i64 timestamp;
  9: PostType post_type;
}

service UniqueIdService {
  i64 ComposeUniqueId (
      1: i64 req_id,
      2: PostType post_type
  ) throws (1: ServiceException se)
}

service TextService {
  TextServiceReturn ComposeText (
      1: i64 req_id,
      2: string text
  ) throws (1: ServiceException se)
}

service UserService {
  void RegisterUser (
      1: i64 req_id,
      2: string first_name,
      3: string last_name,
      4: string username,
      5: string password
  ) throws (1: ServiceException se)

  void RegisterUserWithId (
      1: i64 req_id,
      2: string first_name,
      3: string last_name,
      4: string username,
      5: string password,
      6: i64 user_id
  ) throws (1: ServiceException se)

  string Login(
      1: i64 req_id,
      2: string username,
      3: string password
  ) throws (1: ServiceException se)

  Creator ComposeCreatorWithUserId(
      1: i64 req_id,
      2: i64 user_id,
      3: string username
  ) throws (1: ServiceException se)

  Creator ComposeCreatorWithUsername(
      1: i64 req_id,
      2: string username
  ) throws (1: ServiceException se)

  i64 GetUserId(
      1: i64 req_id,
      2: string username
  ) throws (1: ServiceException se)
}

service ComposePostService {
  void ComposePost(
    1: i64 req_id,
    2: string username,
    3: i64 user_id,
    4: string text,
    5: list<i64> media_ids, 
    6: list<string> media_types,
    7: PostType post_type
  ) throws (1: ServiceException se)

  void StorePost(
    1: i64 req_id,
    2: Post post
  ) throws (1: ServiceException se)

  Post ReadPost(
    1: i64 req_id,
    2: i64 post_id
  ) throws (1: ServiceException se)

  list<Post> ReadPosts(
    1: i64 req_id,
    2: list<i64> post_ids
  ) throws (1: ServiceException se)

  list<Post> ReadUserTimeline(
    1: i64 req_id,
    2: i64 user_id,
    3: i32 start,
    4: i32 stop
  ) throws (1: ServiceException se)

  string Login(
      1: i64 req_id,
      2: string username,
      3: string password
  ) throws (1: ServiceException se)

  void RegisterUser (
      1: i64 req_id,
      2: string first_name,
      3: string last_name,
      4: string username,
      5: string password
  ) throws (1: ServiceException se)

  void RegisterUserWithId (
      1: i64 req_id,
      2: string first_name,
      3: string last_name,
      4: string username,
      5: string password,
      6: i64 user_id
  ) throws (1: ServiceException se)

  list<i64> GetFollowers(
      1: i64 req_id,
      2: i64 user_id
  ) throws (1: ServiceException se)

  void Unfollow(
      1: i64 req_id,
      2: i64 user_id,
      3: i64 followee_id
  ) throws (1: ServiceException se)

  void UnfollowWithUsername(
      1: i64 req_id,
      2: string user_usernmae,
      3: string followee_username
  ) throws (1: ServiceException se)

  void Follow(
      1: i64 req_id,
      2: i64 user_id,
      3: i64 followee_id
  ) throws (1: ServiceException se)

  void FollowWithUsername(
      1: i64 req_id,
      2: string user_usernmae,
      3: string followee_username
  ) throws (1: ServiceException se)

  list<i64> GetFollowees(
      1: i64 req_id,
      2: i64 user_id
  ) throws (1: ServiceException se)

  list<Post> ReadHomeTimeline(
    1: i64 req_id,
    2: i64 user_id,
    3: i32 start,
    4: i32 stop
  ) throws (1: ServiceException se)
}

service PostStorageService {
  void StorePost(
    1: i64 req_id,
    2: Post post
  ) throws (1: ServiceException se)

  Post ReadPost(
    1: i64 req_id,
    2: i64 post_id
  ) throws (1: ServiceException se)

  list<Post> ReadPosts(
    1: i64 req_id,
    2: list<i64> post_ids
  ) throws (1: ServiceException se)
}

service HomeTimelineService {
  list<Post> ReadHomeTimeline(
    1: i64 req_id,
    2: i64 user_id,
    3: i32 start,
    4: i32 stop
  ) throws (1: ServiceException se)

  void WriteHomeTimeline(
    1: i64 req_id,
    2: i64 post_id,
    3: i64 user_id,
    4: i64 timestamp,
    5: list<i64> user_mentions_id
  ) throws (1: ServiceException se)
}

service UserTimelineService {
  void WriteUserTimeline(
    1: i64 req_id,
    2: i64 post_id,
    3: i64 user_id,
    4: i64 timestamp
  ) throws (1: ServiceException se)

  list<Post> ReadUserTimeline(
    1: i64 req_id,
    2: i64 user_id,
    3: i32 start,
    4: i32 stop
  ) throws (1: ServiceException se)
}

service SocialGraphService{
  list<i64> GetFollowers(
      1: i64 req_id,
      2: i64 user_id
  ) throws (1: ServiceException se)

  list<i64> GetFollowees(
      1: i64 req_id,
      2: i64 user_id
  ) throws (1: ServiceException se)

  void Follow(
      1: i64 req_id,
      2: i64 user_id,
      3: i64 followee_id
  ) throws (1: ServiceException se)

  void Unfollow(
      1: i64 req_id,
      2: i64 user_id,
      3: i64 followee_id
  ) throws (1: ServiceException se)

  void FollowWithUsername(
      1: i64 req_id,
      2: string user_usernmae,
      3: string followee_username
  ) throws (1: ServiceException se)

  void UnfollowWithUsername(
      1: i64 req_id,
      2: string user_usernmae,
      3: string followee_username
  ) throws (1: ServiceException se)

  void InsertUser(
      1: i64 req_id,
      2: i64 user_id
  ) throws (1: ServiceException se)
}

service UserMentionService {
  list<UserMention> ComposeUserMentions(
      1: i64 req_id,
      2: list<string> usernames
  ) throws (1: ServiceException se)
}

service UrlShortenService {
  list<Url> ComposeUrls(
      1: i64 req_id,
      2: list<string> urls
  ) throws (1: ServiceException se)

  list<string> GetExtendedUrls(
      1: i64 req_id,
      2: list<string> shortened_urls
  ) throws (1: ServiceException se)
}

service MediaService {
  list<Media> ComposeMedia(
      1: i64 req_id,
      2: list<string> media_types,
      3: list<i64> media_ids
  ) throws (1: ServiceException se)
}

service FrontEndProxy {
  list<Post> ReadHomeTimeline(
    1: i64 req_id,
    2: i64 user_id,
    3: i32 start,
    4: i32 stop
  ) throws (1: ServiceException se)

  void ComposePost(
    1: i64 req_id,
    2: string username,
    3: i64 user_id,
    4: string text,
    5: list<i64> media_ids,
    6: list<string> media_types,
    7: PostType post_type
  ) throws (1: ServiceException se)

  list<i64> GetFollowers(
      1: i64 req_id,
      2: i64 user_id
  ) throws (1: ServiceException se)

  void Unfollow(
      1: i64 req_id,
      2: i64 user_id,
      3: i64 followee_id
  ) throws (1: ServiceException se)

  void UnfollowWithUsername(
      1: i64 req_id,
      2: string user_usernmae,
      3: string followee_username
  ) throws (1: ServiceException se)

  string Login(
      1: i64 req_id,
      2: string username,
      3: string password
  ) throws (1: ServiceException se)

  void Follow(
      1: i64 req_id,
      2: i64 user_id,
      3: i64 followee_id
  ) throws (1: ServiceException se)

  void FollowWithUsername(
      1: i64 req_id,
      2: string user_usernmae,
      3: string followee_username
  ) throws (1: ServiceException se)

  void RegisterUser (
      1: i64 req_id,
      2: string first_name,
      3: string last_name,
      4: string username,
      5: string password
  ) throws (1: ServiceException se)

  list<i64> GetFollowees(
      1: i64 req_id,
      2: i64 user_id
  ) throws (1: ServiceException se)

  list<Post> ReadUserTimeline(
    1: i64 req_id,
    2: i64 user_id,
    3: i32 start,
    4: i32 stop
  ) throws (1: ServiceException se)

  void RegisterUserWithId (
      1: i64 req_id,
      2: string first_name,
      3: string last_name,
      4: string username,
      5: string password,
      6: i64 user_id
  ) throws (1: ServiceException se)
}