%%%-------------------------------------------------------------------
%%% @author Ao Song
%%% @copyright (C) 2018
%%% @doc
%%%
%%% @end
%%% Created : 2018-01-18
%%%-------------------------------------------------------------------
-module(node_manager).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    connected_nodes = []   :: list(),
    enrolled_nodes = []    :: list()
}).

-record(node, {
    name            :: node(),
    is_enrolled     :: boolean()
}).

%%====================================================================
%% API functions
%%====================================================================
-spec start_link(term()) -> {ok, pid()}.
start_link(_Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec is_single_node() -> boolean().
is_single_node() ->
    gen_server:call(?MODULE, is_single_node).

-spec get_enrolled_nodes_number() -> integer().
get_enrolled_nodes_number() ->
    gen_server:call(?MODULE, get_enrolled_nodes_number).

-spec get_connected_nodes_number() -> integer().
get_connected_nodes_number() ->
    gen_server:call(?MODULE, get_connected_nodes_number).

-spec call_nodes(term(), term()) -> ok | {error, term()}.
call_nodes(Event, Message) ->
    gen_server:call(?MODULE, {call_nodes, Event, Message}).



%%====================================================================
%% gen_server callbacks
%%====================================================================
-spec init(list()) -> {ok, #state{}}.
init([]) ->
    {ok, #state{}}.

-spec handle_call(term(), term(), #state{}) -> {reply, term(), #state{}}.
handle_call(is_single_node, _From, State) ->
    EnrolledNodesNum = erlang:length(State#state.enrolled_nodes),
    {reply, (EnrolledNodesNum =:= 0), State};
handle_call(get_enrolled_nodes_number, _From, State) ->
    {reply, erlang:length(State#state.enrolled_nodes), State};
handle_call(get_connected_nodes_number, _From, State) ->
    {reply, erlang:length(State#state.connected_nodes), State};
handle_call({call_nodes, Event, Message}, _From, 
            State#state{enrolled_nodes = Nodes}) ->
    {Replies, BadNodes} = 
        rpc:multi_server_call(Nodes, node_manager, {Event, Message}),
    {reply, {Replies, BadNodes}, State};
handle_call(_Message, _From, State) ->
    Response = ok,
    {reply, Response, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Message, State) ->
    {noreply, State}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({From, {send_request_vote_messages, RequestVoteRPC}}, State) ->
    Reply = leader_election:handle_request_vote_message(RequestVoteRPC);
    From ! {?MODULE, erlang:node(), Reply};
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), #state{}, term()) -> {ok, #state{}}.
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
