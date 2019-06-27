%%%-------------------------------------------------------------------
%%% @author Ao Song
%%% @copyright (C) 2018
%%% @doc
%%%
%%% @end
%%% Created : 2018-01-16
%%%-------------------------------------------------------------------
-module(raft_leader_election).

-behaviour(gen_statem).

%% API
-export([start_link/0]).

%% gen_statem callbacks
-export([
         init/1,
         callback_mode/0,
         format_status/2,
         follower/3,
         handle_event/4,
         terminate/3,
         code_change/4
        ]).

-define(SERVER, ?MODULE).
-define(DEFAULT_HEARTBEAT_INTERVAL, 75).
-define(DEFAULT_ELECTION_TIMEOUT_MIN, 150).
-define(DEFAULT_ELECTION_TIMEOUT_MAX, 300).
-define(MAJORITY, 0.5).

-record(state, {
    %% Persistent state on all servers
    currentTerm = 0,
    votedFor = null,
    log = [],
    %% Volatile state on all servers
    commitIndex = 0,
    lastApplied = 0,
    %% Volatile state on leaders
    nextIndex,
    matchIndex,

    voteCount = 0,
    id
}).

-record(requestVoteRPC, {
    term,  %% Candidate's term
    candidateId,  %% Candidate requesting vote
    lastLogIndex,  %% Index of candidate's last log entry
    lastLogTerm  %% Term of candidate's last log entry
}).

-record(requestVoteRPCResult, {
    term,  %% Current term, for candidate to update itself
    voteGranted  %% true means candidate received vote
}).

-record(appendEntriesRPC, {
    term,
    leaderId,
    prevLogIndex,
    prevLogTerm,
    entries = [],
    leaderCommit
}).

-record(appendEntriesRPCResult, {
    term,
    success
}).

-record(log, {
    term,
    data
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_statem process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

start_election() ->
    gen_statem:call(?SERVER, start_election).

-spec handle_request_vote_message(term()) -> #requestVoteRPCResult{}.
handle_request_vote_message(RequestVoteRPC) ->
    gen_statem:call(?SERVER, {handle_request_vote_message, RequestVoteRPC}).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_statem is started using gen_statem:start/[3,4] or
%% gen_statem:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% When servers start up, they begin as followers.
%%
%% @spec init(Args) -> {CallbackMode, StateName, State} |
%%                     {CallbackMode, StateName, State, Actions} |
%%                     ignore |
%%                     {stop, StopReason}
%%
%% @end
%%--------------------------------------------------------------------
init([]) ->
    set_election_timeout(),
    {ok, follower, #state{}}.

callback_mode() -> state_functions.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Called (1) whenever sys:get_status/1,2 is called by gen_statem or
%% (2) when gen_statem terminates abnormally.
%% This callback is optional.
%%
%% @spec format_status(Opt, [PDict, StateName, State]) -> term()
%% @end
%%--------------------------------------------------------------------
format_status(_Opt, [_PDict, _StateName, _State]) ->
    Status = some_term,
    Status.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name.  If callback_mode is statefunctions, one of these
%% functions is called when gen_statem receives and event from
%% call/2, cast/2, or as a normal process message.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Actions} |
%%                   {stop, Reason, NewState} |
%%                   stop |
%%                   {stop, Reason :: term()} |
%%                   {stop, Reason :: term(), NewData :: data()} |
%%                   {stop_and_reply, Reason, Replies} |
%%                   {stop_and_reply, Reason, Replies, NewState} |
%%                   {keep_state, NewData :: data()} |
%%                   {keep_state, NewState, Actions} |
%%                   keep_state_and_data |
%%                   {keep_state_and_data, Actions}
%% @end
%%--------------------------------------------------------------------
follower(info, {timeout, _TimerRef, electionTimeout}, State) ->
    %% If a follower receives no communication over a period of time, a new election begins.
    set_election_timeout(),
    {next_state, candidate, State#state{voteCount = 1}};
follower({call, From},
         {handle_request_vote_message,
          RequestVoteRPC#requestVoteRPC{term = CandidateTerm,
                                        candidateId = CandidateId}},
         State = #state{currentTerm = FollowerCurrentTerm,
                        votedFor = VotedFor,
                        log = [],
                        electionTimeoutRef = Ref})
  when (VotedFor == null) orelse (VotedFor == CandidateId) ->
    set_election_timeout(),
    case (FollowerCurrentTerm =< CandidateTerm) of
        true ->            
            {next_state, follower,
             State#state{votedFor = CandidateId},
             [{reply, From, #requestVoteRPCResult{term = FollowerCurrentTerm,
                                                  voteGranted = true}}]};
        false ->
            {next_state, follower,
             State#state{votedFor = null}, 
             [{reply, From, #requestVoteRPCResult{term = FollowerCurrentTerm,
                                                  voteGranted = false}}]}
    end;
%% vote!
follower({call, From}, 
         {handle_request_vote_message, 
          RequestVoteRPC#requestVoteRPC{term = CandidateTerm,
                                        candidateId = CandidateId,
                                        lastLogIndex = CandidateLastLogIndex,
                                        lastLogTerm = CandidateLastLogTerm}}, 
         State = #state{currentTerm = FollowerCurrentTerm,
                        votedFor = VotedFor,
                        log = FollowerLog,
                        lastApplied = FollowerLastApplied,
                        electionTimeoutRef = Ref}) 
  when (VotedFor == null) orelse (VotedFor == CandidateId) ->
    case (FollowerCurrentTerm =< CandidateTerm) andalso
         (lists:last(FollowerLog)#log.term =< CandidateLastLogTerm) andalso
         (FollowerLastApplied =< CandidateLastLogIndex) of
        true -> 
            set_election_timeout(),
            {next_state, follower, 
             State#state{votedFor = CandidateId}, 
             [{reply, From, #requestVoteRPCResult{term = FollowerCurrentTerm,
                                                  voteGranted = true}}]};
        false ->
            {next_state, follower, 
             State#state{votedFor = null}, 
             [{reply, From, #requestVoteRPCResult{term = FollowerCurrentTerm,
                                                  voteGranted = false}}]}
    end;    
%% other cases in vote
follower({call, From}, {handle_request_vote_message, _RequestVoteRPC}, 
         _State) ->
    {reply, From, #requestVoteRPCResult{term = FollowerCurrentTerm,
                                        voteGranted = false}};
%% a server remains in follower state as long as it receives valid RPCs from
%% a leader or candidate.
follower({call, From}, {handle_request_vote_message, _RequestVoteRPC}, 
         _State) ->
    {reply, From, #requestVoteRPCResult{term = FollowerCurrentTerm,
                                        voteGranted = false}};
%% put it down for a while!
follower(_EventType, _EventContent, State) ->
    case node_manager:is_single_node() of
        true -> 
            {next_state, leader, State};
        false ->
            {keep_state, State}
    end.

%% election timer elapses, start new election
candidate(info, {timeout, _TimerRef, electionTimeout}, State) ->
    set_election_timeout(),
    {next_state, candidate, State#state{voteCount = 1}};
%% todo, candidate convert to follower when receive AppendEntries RPC from new leader
%% initial vote
candidate(_EventType, _EventContent,
          State = #state{log = [],
                         electionTimeoutRef = Ref}) ->
    RequestVoteRPC = #requestVoteRPC{term = State#state.currentTerm + 1,
                                     candidateId = State#state.id,
                                     lastLogIndex = 0,
                                     lastLogTerm = 0},
    set_election_timeout(),
    {Replies, _BadNodes} = send_request_vote_messages(RequestVoteRPC),
    case is_elected(Replies) of
        true -> 
            {next_state, leader, 
             State#state{term = State#state.currentTerm + 1}};
        false ->
            {next_state, candidate, State#state{electionTimeoutRef = NewRef}}
    end;
candidate(_EventType, _EventContent, State#state{log = Log,
                                                 electionTimeoutRef = Ref}) ->
    LastLog = lists:last(Log),
    RequestVoteRPC = #requestVoteRPC{term = State#state.currentTerm + 1,
                                     candidateId = State#state.id,
                                     lastLogIndex = LastLog#log.term,
                                     lastLogTerm = State#state.lastApplied},
    set_election_timeout(),
    {Replies, _BadNodes} = send_request_vote_messages(RequestVoteRPC),
    case is_elected(Replies) of
        true ->
            {next_state, leader, 
             State#state{term = State#state.currentTerm + 1}};
        false ->
            {next_state, candidate, State#state{electionTimeoutRef = NewRef}}
    end.

leader(_EventType, {timeout, _TimerRef, leaderHeartbeat}, State) ->
    %% Leaders send periodic heartbeats to all followers to maintain their authority.
    broadcast_heartbeat_to_nodes(),
    start_heartbeat_timer(),
    {next_state, leader, State};
leader(_EventType, _EventContent, State) ->
    NextStateName = next_state,
    {next_state, NextStateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% If callback_mode is handle_event_function, then whenever a
%% gen_statem receives an event from call/2, cast/2, or as a normal
%% process message, this function is called.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Actions} |
%%                   {stop, Reason, NewState} |
%%                   stop |
%%                   {stop, Reason :: term()} |
%%                   {stop, Reason :: term(), NewData :: data()} |
%%                   {stop_and_reply, Reason, Replies} |
%%                   {stop_and_reply, Reason, Replies, NewState} |
%%                   {keep_state, NewData :: data()} |
%%                   {keep_state, NewState, Actions} |
%%                   keep_state_and_data |
%%                   {keep_state_and_data, Actions}
%% @end
%%--------------------------------------------------------------------
handle_event(_EventType, _EventContent, _StateName, State) ->
    NextStateName = the_next_state_name,
    {next_state, NextStateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% get election timeout, unit in ms
get_election_timeout() ->
    random_number(?DEFAULT_ELECTION_TIMEOUT_MIN, ?DEFAULT_ELECTION_TIMEOUT_MAX).

%% generate a random number between From and To
random_number(From, To) ->
    _SetSeed = crypto:rand_seed(),
    round(From + To * rand:uniform()).

send_request_vote_messages(RequestVoteRPC) ->
    node_manager:call_nodes(send_request_vote_messages, RequestVoteRPC).

is_elected([]) ->
    false;
is_elected(Replies) ->
    Voted = [R || R <- Replies, R#requestVoteRPCResult.voteGranted == true],
    Rate = erlang:length(Voted) / node_manager:get_enrolled_nodes_number(),
    Rate > ?MAJORITY.

set_election_timeout() ->
    ElectionTimeout = get_election_timeout(),
    erlang:start_timer(ElectionTimeout, self(), electionTimeout).

start_heartbeat_timer() ->
    erlang:start_timer(?DEFAULT_HEARTBEAT_INTERVAL, self(), leaderHeartbeat).

broadcast_heartbeat_to_nodes() ->
    raft_nodes_manager:broadcast_msg(leader_alive).