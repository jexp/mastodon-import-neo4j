// Mastodon to Neo4j import

// https://docs.joinmastodon.org/client/public/
// https://docs.joinmastodon.org/api/rate-limits/

// call apoc.load.json("https://mastodon.social/api/v1/timelines/public")
// server peers https://mastodon.social//api/v1/instance/peers
// users https://mastodon.social//api/v1/directory?limit=2
// tags https://mastodon.social/api/v1/timelines/tag/tagname


// tag::constraint[]
create constraint message_id if not exists FOR (m:Message) REQUIRE (m.id) is unique;
create constraint user_id if not exists FOR (u:User) REQUIRE (u.acct) is unique;
create constraint tag_name if not exists FOR (t:Tag) REQUIRE (t.name) is unique;
// end::constraint[]

// tag::timeline[]
:auto unwind range(0,4000,40) as skip
call { with skip
call apoc.load.json("https://mastodon.social/api/v1/timelines/public?limit=40&skip="+skip) yield value
unwind value as v
WITH v, v.account as a
MERGE (m:Message {id:v.id})
ON CREATE SET m += v {.sensitive, .language, .uri, .replies_count, .favourites_count, .content },
m.created_at = datetime(v.created_at)

MERGE (u:User {acct:a.acct})
ON CREATE SET u += a {.id, .locked, .bot, .discoverable, .group, .note, .url, .avatar, 
.followers_count, .following_count, .statuses_count }, 
u.created_at = datetime(a.created_at),
u.last_status_at = date(a.last_status_at),
u.twitter = [f IN a.fields WHERE f.name = 'Twitter' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.pronouns = [f IN a.fields WHERE f.name = 'Pronouns' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.website = [f IN a.fields WHERE f.name IN ['Website','Homepage','Site','Web'] | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.github = [f IN a.fields WHERE f.name = 'Github' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.reddit = [f IN a.fields WHERE f.name = 'Reddit' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.location = [f IN a.fields WHERE f.name = 'Location' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.lang = [f IN a.fields WHERE f.name = 'Lang' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.support = [f IN a.fields WHERE f.name = 'Support' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0]

MERGE (u)-[:POSTED]->(m)
WITH * 
call { with v,m
    UNWIND v.tags as t
    MERGE (tag:Tag {name:t.name}) // ON CREATE SET tag.url = t.url
    MERGE (m)-[:TAGGED]->(tag)
    RETURN count(*) as tags
}

WITH * WHERE v.in_reply_to_id is not null
MERGE (reply:Message {id:v.in_reply_to_id})
MERGE (m)-[:REPLIED_TO]->(reply)
} in transactions of 1 row;
// end::timeline[]

// tag::tags[]
:auto MATCH (t:Tag)
WITH t.name as name
call { with name
call apoc.load.json("https://mastodon.social/api/v1/timelines/tag/+"+name) yield value
unwind value as v
WITH v, v.account as a
MERGE (m:Message {id:v.id})
ON CREATE SET m += v {.sensitive, .language, .uri, .replies_count, .favourites_count, .content },
m.created_at = datetime(v.created_at)

MERGE (u:User {acct:a.acct})
ON CREATE SET u += a {.id, .locked, .bot, .discoverable, .group, .note, .url, .avatar, 
.followers_count, .following_count, .statuses_count }, 
u.created_at = datetime(a.created_at),
u.last_status_at = date(a.last_status_at),
u.twitter = [f IN a.fields WHERE f.name = 'Twitter' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.pronouns = [f IN a.fields WHERE f.name = 'Pronouns' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.website = [f IN a.fields WHERE f.name IN ['Website','Homepage','Site','Web'] | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.github = [f IN a.fields WHERE f.name = 'Github' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.reddit = [f IN a.fields WHERE f.name = 'Reddit' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.location = [f IN a.fields WHERE f.name = 'Location' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.lang = [f IN a.fields WHERE f.name = 'Lang' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.support = [f IN a.fields WHERE f.name = 'Support' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0]

MERGE (u)-[:POSTED]->(m)
WITH * 
call { with v,m
    UNWIND v.tags as t
    MERGE (tag:Tag {name:t.name}) // ON CREATE SET tag.url = t.url
    MERGE (m)-[:TAGGED]->(tag)
    RETURN count(*) as tags
}

WITH * WHERE v.in_reply_to_id is not null
MERGE (reply:Message {id:v.in_reply_to_id})
MERGE (m)-[:REPLIED_TO]->(reply)
} in transactions of 1 row;
// end::tags[]


// tag::users[]
:auto unwind range(0,4000,40) as skip
call { with skip
call apoc.load.json("https://mastodon.social//api/v1/directory?limit=40&skip="+skip) yield value as a

MERGE (u:User {acct:a.acct})
ON CREATE SET u += a {.id, .locked, .bot, .discoverable, .group, .note, .url, .avatar, 
.followers_count, .following_count, .statuses_count }, 
u.created_at = datetime(a.created_at),
u.last_status_at = date(a.last_status_at),
u.twitter = [f IN a.fields WHERE f.name = 'Twitter' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.pronouns = [f IN a.fields WHERE f.name = 'Pronouns' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.website = [f IN a.fields WHERE f.name IN ['Website','Homepage','Site','Web'] | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.github = [f IN a.fields WHERE f.name = 'Github' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.reddit = [f IN a.fields WHERE f.name = 'Reddit' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.location = [f IN a.fields WHERE f.name = 'Location' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.lang = [f IN a.fields WHERE f.name = 'Lang' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0],
u.support = [f IN a.fields WHERE f.name = 'Support' | apoc.text.regexGroups(f.value, 'href="([^"]+)"')[0][1]][0]
} in transactions of 1 row;
// end::users[]
