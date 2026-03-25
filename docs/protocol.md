# TODO: Define wire protocol commands, responses, delimiters, and error formats.

# Protocol Specification

## Transport
- TCP (SOCK_STREAM), TLS 1.2+, Port 9999

## Message Format
```
<COMMAND>[|<ARG1>|...|<ARGN>]\n
```
- Newline terminated (`\n`)
- Pipe separated (`|`)
- UTF-8 encoded

## Commands

### Client → Server
| Command | Format |
|---------|--------|
| Submit score | `SUBMIT_SCORE\|player\|score\|unix_timestamp` |
| Get leaderboard | `GET_LEADERBOARD` |
| Ping | `PING` |

### Server → Client
| Message | Format |
|---------|--------|
| Welcome | `WELCOME\|[json_array]` |
| Accepted | `OK\|Score accepted` |
| Rejected (stale) | `OK\|Score rejected (stale, LWW)` |
| Error | `ERROR\|reason` |
| Pong | `PONG` |
| Leaderboard | `LEADERBOARD\|[json_array]` |
| Broadcast | `LEADERBOARD_UPDATE\|[json_array]` |

## Conflict Resolution (LWW)
```
Accept if:  incoming_timestamp > stored_timestamp
Reject if:  incoming_timestamp <= stored_timestamp
```

## Session Flow
```
Client          Server
  |──TCP+TLS──▶   |
  |◀──WELCOME──   |   (current leaderboard)
  |──SUBMIT────▶  |
  |◀──OK────────  |
  |              broadcasts LEADERBOARD_UPDATE to ALL clients
  |──GET────────▶ |
  |◀──LEADERBOARD |
  |──PING───────▶ |
  |◀──PONG──────  |
```
