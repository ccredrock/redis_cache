%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(redis_cache).

-export([start/0, stop/0]).

-export([start_link/1]).

-export([put_val/1,     %% 更新
         put_val/3,     %% 更新
         put_val/4,     %% 更新
         del_val/1,     %% 删除
         del_val/2,     %% 删除
         set_val/1,     %% 更新 || 删除
         get_val/2,     %% 获取
         get_val/3]).   %% 获取

-export([purge/0,       %% 净化系统
         clean/0,       %% 清理数据
         clean/1,       %% 清理数据
         reload/0,      %% 重新加载
         reload/1]).    %% 重新加载

-export([get_redis_notice_len/0,
         get_cache_notice_len/0,
         get_table_vals/1,
         get_redis_vals/1]).

-export([lock/0, unlock/0, get_locks/0]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(TIMEOUT, 500).

-record(state, {tables = [], notice_len = 0, reduce_max = 0, reduce_len = 0, locks = []}).

-define(BIN(V), to_binary(V)).

-define(REDIS_TABLE(L), iolist_to_binary([<<"$redis_cache_kv#">> | L])).
-define(ETS_TABLE(L), list_to_atom("$redis_cache_kv#" ++ atom_to_list(L))).

-define(NOTICE, <<"$redis_cache_notice">>).

-define(REDUCE_MAX, 1000).
-define(REDUCE_LEN(X), (X div 4 * 3)).

-define(RED_LUA(Max, Red),
        iolist_to_binary([<<"local Len = redis.pcall('LLEN', '$redis_cache_notice')
                            if Len >= ">>, ?BIN(Max), <<" then
                                local To = Len - math.modf(Len / ">>, ?BIN(Red), <<") * ">>, ?BIN(Red), <<"- 1
                                return redis.pcall('LTRIM', '$redis_cache_notice', 0, To)
                            else
                                return
                            end">>])).

%%------------------------------------------------------------------------------
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

%%------------------------------------------------------------------------------
-spec start_link([atom()]) -> {ok, pid()} | ignore | {error, any()}.
start_link(Tables) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Tables], []).

-spec put_val(atom(), binary(), any(), any()) -> ok | {'EXIT', any()}.
put_val(Table, Key, CKey, CVal) ->
    put_val(Table, Key, [{CKey, CVal}]).

%% [{ckey, cval}]
put_val(Table, Key, MKV) ->
    put_val([{Table, Key, MKV}]).

%% [{table, key, [{ckey, cval}]}]
put_val(List) ->
    set_val([{put_val, T, K, M} || {T, K, M} <- List]).

del_val(Table, Key) ->
    del_val([{Table, Key}]).

%% [{table, key}]
del_val(List) ->
    set_val([{del_val, T, K} || {T, K} <- List]).

%% [{op, ...}]
set_val(List) ->
    gen_server:call(?MODULE, {set_val, List}).

get_val(Table, Key) ->
    case ets:lookup(?ETS_TABLE(Table), Key) of
        [{Key, Map}] -> Map;
        _ -> null
    end.

get_val(Table, Key, CKey) ->
    case get_val(Table, Key) of
        null -> null;
        Map -> maps:get(CKey, Map, null)
    end.

reload() -> reload('$all').
reload(Table) when is_atom(Table) -> reload([Table]);
reload(List) -> gen_server:call(?MODULE, {reload, List}).

clean() -> clean('$all').
clean(Table) when is_atom(Table) -> clean([Table]);
clean(List) -> gen_server:call(?MODULE, {clean, List}).

purge() ->
    {ok, Tables} = application:get_env(redis_cache, tables),
    {ok, _} = eredis_pool:q([<<"DEL">>, ?NOTICE]),
    do_clean_table(Tables), ok.

get_redis_notice_len() ->
    {ok, BLen} = eredis_pool:q([<<"LLEN">>, ?NOTICE]), binary_to_integer(BLen).

get_cache_notice_len() ->
    State = sys:get_state(?MODULE), State#state.notice_len.

get_table_vals(Table) ->
    ets:tab2list(?ETS_TABLE(Table)).

get_redis_vals(Table) ->
    {ok, KeyList} = eredis_pool:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), <<"*">>])]),
    [begin
         {ok, Vals} = eredis_pool:q([<<"HGETALL">>, ?REDIS_TABLE([?BIN(Table), "@", ?BIN(Key)])]),
         {Key, Vals}
     end || Key <- KeyList].

get_locks() ->
    State = sys:get_state(?MODULE), State#state.locks.

lock() ->
    gen_server:call(?MODULE, lock).

unlock() ->
    gen_server:call(?MODULE, unlock).

%%------------------------------------------------------------------------------
init([Tables]) ->
    Max = application:get_env(redis_cache, reduce_max, ?REDUCE_MAX),
    [do_load_table(Table) || Table <- Tables],
    {ok, #state{tables = Tables, reduce_max = Max, reduce_len = ?REDUCE_LEN(Max)}, 0}.

handle_call({set_val, List}, {Master, _}, State) ->
    {State1, Result} = do_set_val(Master, State, List),
    {reply, Result, State1};
handle_call({reload, ['$all']}, _From, State) ->
    {reply, catch do_reload_table(State#state.tables), State};
handle_call({reload, List}, _From, State) ->
    {reply, catch do_reload_table(List), State};
handle_call({clean, ['$all']}, _From, State) ->
    {reply, catch do_clean_table(State#state.tables), State};
handle_call({clean, List}, _From, State) ->
    {reply, catch do_clean_table(List), State};
handle_call(lock, {Master, _}, State) ->
    {State1, Result} = do_lock(Master, State),
    {reply, Result, State1};
handle_call(unlock, {Master, _}, State) ->
    {reply, ok, do_unlock(Master, State)};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(timeout, State) ->
    State1 = do_timeout(State),
    erlang:send_after(?TIMEOUT, self(), timeout),
    {noreply, State1};
handle_info({'DOWN', _Ref, process, PID, _Reason}, State) ->
    {noreply, do_check_deads(PID, State)};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_integer(X) -> integer_to_binary(X);
to_binary(X) when is_binary(X)  -> X.

get_type(X) when is_binary(X) -> <<"binary">>;
get_type(X) when is_list(X) -> <<"list">>;
get_type(X) when is_integer(X) -> <<"integer">>;
get_type(X) when is_atom(X) -> <<"atom">>.

put_type(X, binary) -> X;
put_type(X, <<"binary">>) -> X;
put_type(X, list) -> binary_to_list(X);
put_type(X, <<"list">>) -> binary_to_list(X);
put_type(X, integer) -> binary_to_integer(X);
put_type(X, <<"integer">>) -> binary_to_integer(X);
put_type(X, atom) -> list_to_atom(binary_to_list(X));
put_type(X, <<"atom">>) -> list_to_atom(binary_to_list(X)).

encode(V) ->
    jsx:encode(#{<<"type">> => get_type(V), <<"val">> => to_binary(V)}).

decode(B) ->
    #{<<"type">> := T, <<"val">> := V} = jsx:decode(B, [return_maps]),
    put_type(V, T).

%%------------------------------------------------------------------------------
do_load_table(Table) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:new(EtsTable, [named_table, public, {read_concurrency, true}]),
    {ok, List} = eredis_pool:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), <<"*">>])]),
    [do_load_table_key(EtsTable, RedisTable) || RedisTable <- List].

do_load_table_key(EtsTable, RedisTable) ->
    {ok, List} = eredis_pool:q([<<"HGETALL">>, RedisTable]),
    {_Table, EtsKey} = do_parse_redis_table(RedisTable),
    ets:insert(EtsTable, {EtsKey, do_form_map(List, maps:new())}).

do_form_map([Key, Val | T], Map) ->
    do_form_map(T, Map#{decode(Key) => decode(Val)});
do_form_map([], Map) -> Map.

do_reload_table([Table | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:delete_all_objects(EtsTable),
    {ok, List} = eredis_pool:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), <<"*">>])]),
    [do_load_table_key(EtsTable, RedisTable) || RedisTable <- List],
    do_reload_table(T);
do_reload_table([]) -> ok.

do_clean_table([Table | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:info(EtsTable) =/= undefined andalso ets:delete_all_objects(EtsTable),
    {ok, List} = eredis_pool:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), <<"*">>])]),
    [{ok, _} = eredis_pool:q([<<"DEL">>, RedisTable]) || RedisTable <- List],
    do_clean_table(T);
do_clean_table([]) -> ok.

%%------------------------------------------------------------------------------
do_set_val(_Master, State, []) -> {State, ok};
do_set_val(Master, #state{locks = Locks} = State, List) ->
    case lists:keyfind(Master, 1, Locks) of
        false ->
            try
                do_set_redis(null, List, []),
                do_set_ets(List),
                {State, ok}
            catch
                E:R -> {State,{error, {E, R, erlang:get_stacktrace()}}}
            end;
        {Master, _Pool, Worker} ->
            try
                do_set_redis(Worker, List, []),
                do_set_ets(List),
                {do_unlock(Master, State), ok}
            catch
                E:R -> {do_unlock(Master, State), {error, {E, R, erlang:get_stacktrace()}}}
            end
        end.

do_set_redis(Worker, [{put_val, Table, Key, MKV} | T], Acc) ->
    do_set_redis(Worker,T, Acc ++ do_form_put(Table, Key, MKV, []));
do_set_redis(Worker, [{del_val, Table, Key} | T], Acc) ->
    do_set_redis(Worker,T, Acc ++ do_form_del(Table, Key) ++ Acc);
do_set_redis(null, [], Acc) -> {ok, _} = eredis_pool:transaction(Acc);
do_set_redis(Worker, [], Acc) -> {ok, _} = eredis_pool:lt(Worker, Acc).

do_form_put(Table, Key, [{CK, CV} | T], Acc) ->
    do_form_put(Table, Key, T, [encode(CK), encode(CV)| Acc]);
do_form_put(_Table, _Key, [], []) -> [];
do_form_put(Table, Key, [], Acc) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    [[<<"LPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"put">>, <<"table">> => RedisTable})],
     [<<"HMSET">>, RedisTable] ++ Acc].

do_form_del(Table, Key) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    [[<<"LPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"del">>, <<"table">> => RedisTable})],
     [<<"DEL">>, RedisTable]].

do_set_ets([{put_val, Table, Key, MKV} | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:insert(EtsTable, {Key, do_get_map(EtsTable, Key, MKV)}),
    do_set_ets(T);
do_set_ets([{del_val, Table, Key} | T]) ->
    ets:delete(?ETS_TABLE(Table), Key),
    do_set_ets(T);
do_set_ets([]) -> skip.

do_get_map(Name, Key, MKV) ->
    case ets:lookup(Name, Key) of
        [] -> maps:from_list(MKV);
        [{_Key, Map}] -> maps:merge(Map, maps:from_list(MKV))
    end.

%%------------------------------------------------------------------------------
do_timeout(State) ->
    case catch do_check_update(State) of
        #state{} = NState -> NState;
        {'EXIT', Reason} -> error_logger:error_msg("redis_cache error ~p~n", [{Reason}]), State
    end.

do_check_update(#state{notice_len = Len, reduce_max = Max, reduce_len = RLen, tables = Tables} = State) ->
    case eredis_pool:q([<<"LLEN">>, ?NOTICE]) of
        {ok, NBinLen} ->
            case binary_to_integer(NBinLen) of
                NLen when NLen >= Max ->
                    do_reduce_len(Max, RLen), State;
                NLen when NLen > Len ->
                    do_update_len(Len, NLen, Tables),
                    State#state{notice_len = NLen};
                NLen when NLen < Len andalso NLen > Len rem RLen ->
                    do_update_len(Len rem RLen, NLen, Tables),
                    State#state{notice_len = NLen};
                NLen ->
                    State#state{notice_len = NLen}
            end;
        _ ->
            State
    end.

do_reduce_len(Max, RLen) ->
    {ok, _} = eredis_pool:q([<<"eval">>, ?RED_LUA(Max, RLen), 0]).

do_update_len(Len, NLen, Tables) ->
    {ok, List} = eredis_pool:q([<<"LRANGE">>, ?NOTICE, 0, NLen - Len - 1]),
    [do_update_key(Val, Tables) || Val  <- lists:usort(List)].

do_update_key(Val, Tables) ->
    #{<<"op">> := OP, <<"table">> := RedisTable} =jsx:decode(Val, [return_maps]),
    {Table, EtsKey} = do_parse_redis_table(RedisTable),
    case lists:member(Table, Tables) of
        true ->
            EtsTable = ?ETS_TABLE(Table),
            case OP of
                <<"put">> ->
                    {ok, List} = eredis_pool:q([<<"HGETALL">>, RedisTable]),
                    ets:insert(EtsTable, {EtsKey, do_form_map(List, maps:new())});
                <<"del">> ->
                    ets:delete(EtsTable, EtsKey)
            end;
        false ->
            skip
    end.

do_parse_redis_table(RedisTable) ->
    [<<>>, Left] = binary:split(RedisTable, ?REDIS_TABLE(<<>>)),
    [Table, Left1] = re:split(Left, <<"@">>),
    [Type, BinEtsKey] = re:split(Left1, <<"&">>),
    {put_type(Table, atom), put_type(BinEtsKey, Type)}.

%%------------------------------------------------------------------------------
do_lock(Master, #state{locks = Locks} = State) ->
    try
        lists:keymember(Master, 1, Locks) andalso error(had_lock),
        {ok, Pool, Worker} = do_touch(eredis_pool:lock()),
        {ok, _} = do_touch(eredis_pool:lq(Worker, [<<"WATCH">>, ?NOTICE])),
        State1 = do_timeout(State),
        erlang:monitor(process, Master),
        erlang:monitor(process, Worker),
        {State1#state{locks = [{Master, Pool, Worker} | Locks]}, ok}
    catch
        E:R -> {State, {error, {E, R, erlang:get_stacktrace()}}}
    end.

do_touch({error, Reason}) -> error(Reason);
do_touch(Result) -> Result.

do_check_deads(PID, #state{locks = Locks} = State) ->
    case lists:keymember(PID, 3, Locks) of
        false -> do_unlock(PID, State);
        true -> State#state{locks = lists:keydelete(PID, 3, Locks)}
    end.

do_unlock(PID, #state{locks = Locks} = State) ->
    case lists:keyfind(PID, 1, Locks) of
        false -> State;
        {PID, Pool, Worker} ->
            eredis_pool:lq(Worker, [<<"UNWATCH">>]),
            eredis_pool:unlock(Pool, Worker),
            State#state{locks = lists:keydelete(PID, 1, Locks)}
    end.

