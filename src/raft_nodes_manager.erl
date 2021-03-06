%%%-------------------------------------------------------------------
%%% @author Ao Song
%%% @copyright (C) 2018
%%% @doc
%%%
%%% @end
%%% Created : 2018-01-18
%%%-------------------------------------------------------------------
-module(raft_nodes_manager).

-behaviour(gen_server).

%% API
-export([start_link/1,
         broadcast_msg/1,
         is_single_node/0,
         get_connected_nodes_number/0,
         call_nodes/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    connected_nodes = []   :: list(),
    failed_nodes = []      :: list()
}).

-define(RAFT_CONFIG_FILE, "raft.config").

%%====================================================================
%% API functions
%%====================================================================
start_link(_Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

broadcast_msg(Msg) ->
    gen_server:call(?MODULE, {broadcast_msg, Msg}).

is_single_node() ->
    gen_server:call(?MODULE, is_single_node).

get_connected_nodes_number() ->
    gen_server:call(?MODULE, get_connected_nodes_number).

call_nodes(Event, Message) ->
    gen_server:call(?MODULE, {call_nodes, Event, Message}).



%%====================================================================
%% gen_server callbacks
%%====================================================================
-spec init(list()) -> {ok, #state{}}.
init([]) ->
    case file:consult(?RAFT_CONFIG_FILE) of
        {ok, Configs} ->
            InitialNodes = find_init_nodes_in_config(Configs),
            ConnectedNodes = 
                [N || N <- InitialNodes, pong =:= net_adm:ping(N)],
            FailedNodes = lists:subtract(InitialNodes, ConnectedNodes),
            {ok, #state{connected_nodes = ConnectedNodes,
                        failed_nodes = FailedNodes}};
        {error, _Reason} ->
            {ok, #state{}}
    end.

-spec handle_call(term(), term(), #state{}) -> {reply, term(), #state{}}.
handle_call({broadcast_msg, Msg}, _From, State) ->
    rpc:abcast(State#state.connected_nodes, raft_nodes_manager, Msg),
    {reply, ok, State};
handle_call(is_single_node, _From, State) ->
    {reply, (State#state.connected_nodes =:= []), State};
handle_call(get_connected_nodes_number, _From, State) ->
    {reply, erlang:length(State#state.connected_nodes), State};
handle_call({call_nodes, Event, Message}, _From,
            State = #state{connected_nodes = Nodes}) ->
    {Replies, BadNodes} =
        rpc:multi_server_call(Nodes, raft_nodes_manager, {Event, Message}),
    {reply, {Replies, BadNodes}, State};
handle_call(_Message, _From, State) ->
    Response = ok,
    {reply, Response, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Message, State) ->
    {noreply, State}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({From, {send_request_vote_messages, RequestVoteRPC}}, _State) ->
    Reply = leader_election:handle_request_vote_message(RequestVoteRPC),
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
find_init_nodes_in_config(Configs) ->
    case lists:keyfind(nodes, 1, Configs) of
        {nodes, Nodes} ->
            Nodes;
        false ->
            []
    end.
