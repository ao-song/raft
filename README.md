raft
=====

An implementation of raft protocol.

Utilizing the distributed erlang feature to skip the implementation protocols like gossip.

Start erlang with erl:
-----
    -connect_all should be true

Build:
-----
    $ rebar3 compile

Working state:
-----
    Read nodes info from config file
