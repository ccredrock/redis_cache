%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(redis_cache_copy).

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

-export([get_ref/0,      %% 索引
         rset_val/2,     %% 更新 || 删除
         diff_ref/1,     %% 索引
         rput_val/2,     %% 更新
         rput_val/4,     %% 更新
         rput_val/5,     %% 更新
         rdel_val/2,     %% 删除
         rdel_val/3]).   %% 删除

-export([purge/0,       %% 净化系统
         clean/0,       %% 清理数据
         clean/1,       %% 清理数据
         reload/0,      %% 重新加载
         reload/1]).    %% 重新加载

-export([get_redis_notice_len/0,
         get_cache_notice_len/0,
         get_table_vals/1,
         get_redis_vals/1]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(TIMEOUT, 500).

-define(BIN(V), to_binary(V)).

-define(ETS, '$redis_cache_copy').
-define(ETS_TABLE(L), list_to_atom("$redis_cache_copy_kv#" ++ atom_to_list(L))).
-define(REDIS_TABLE(L), iolist_to_binary([<<"$redis_cache_kv#">> | L])).

-define(NOTICE,  <<"$redis_cache_notice">>).

-define(REDUCE_MAX, 1000).
-define(REDUCE_LEN(X), (X div 4 * 3)).

-define(RED_LUA(Max, Red),
        iolist_to_binary(["local Len = redis.pcall('LLEN', '", ?NOTICE, "')
                           if Len >= ", ?BIN(Max), " then
                                local To = Len - math.modf(Len / ", ?BIN(Red), ") * ", ?BIN(Red), "- 1
                                redis.pcall('LTRIM', '", ?NOTICE, "', 0, To)
                           end"])).

-define(SET_LUA(Len, Exec),
        iolist_to_binary(["if redis.pcall('LLEN', '", ?NOTICE, "') == ", ?BIN(Len), " then ",
                          Exec, " return 'OK'
                          else
                            return 'SKIP'
                          end"])).

-record(state, {tables = [], notice_len = 0, reduce_max = 0, reduce_len = 0}).

%%------------------------------------------------------------------------------
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

%%------------------------------------------------------------------------------
-spec start_link([atom()]) -> {ok, pid()} | ignore | {error, any()}.
start_link(Tables) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Tables], []).

%%------------------------------------------------------------------------------
-spec put_val(atom(), binary(), any(), any()) -> ok | {'EXIT', any()}.
put_val(Table, Key, CKey, CVal) ->
    put_val(Table, Key, [{CKey, CVal}]).

%% [{ckey, cval}]
put_val(Table, Key, MKV) ->
    put_val([{Table, Key, MKV}]).

%% [{table, key, [{ckey, cval}]}]
put_val(List) ->
    set_val([{put, T, K, M} || {T, K, M} <- List]).

del_val(Table, Key) ->
    del_val([{Table, Key}]).

%% [{table, key}]
del_val(List) ->
    set_val([{del, T, K} || {T, K} <- List]).

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

%%------------------------------------------------------------------------------
get_ref() ->
    [{ref, Ref}] = ets:lookup(?ETS, ref), Ref.

%% [{op, ...}]
rset_val(Ref, List) ->
    gen_server:call(?MODULE, {rset_val, Ref, List}).

diff_ref(Ref) ->
    gen_server:call(?MODULE, {diff_ref, Ref}).

-spec rput_val(atom(), binary(), any(), any()) -> ok | {'EXIT', any()}.
rput_val(Ref, Table, Key, CKey, CVal) ->
    put_val(Ref, Table, Key, [{CKey, CVal}]).

%% [{ckey, cval}]
rput_val(Ref, Table, Key, MKV) ->
    rput_val(Ref, [{Table, Key, MKV}]).

%% [{table, key, [{ckey, cval}]}]
rput_val(Ref, List) ->
    rset_val(Ref, [{put, T, K, M} || {T, K, M} <- List]).

rdel_val(Ref, Table, Key) ->
    rdel_val(Ref, [{Table, Key}]).

%% [{table, key}]
rdel_val(Ref, List) ->
    rset_val(Ref, [{del, T, K} || {T, K} <- List]).

%%------------------------------------------------------------------------------
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

%%------------------------------------------------------------------------------
init([Tables]) ->
    Max = application:get_env(redis_cache, reduce_max, ?REDUCE_MAX),
    Len = do_load_len(),
    [do_load_table(Table) || Table <- Tables],
    {ok, #state{tables = Tables, notice_len = Len, reduce_max = Max, reduce_len = ?REDUCE_LEN(Max)}, 0}.

handle_call({set_val, List}, _, State) ->
    {reply, catch do_set_val(List), State};
handle_call({rset_val, Ref, List}, _, State) ->
    {reply, catch do_rset_val(Ref, List), State};
handle_call({reload, ['$all']}, _From, State) ->
    {reply, catch do_reload_table(State#state.tables), State};
handle_call({reload, List}, _From, State) ->
    {reply, catch do_reload_table(List), State};
handle_call({clean, ['$all']}, _From, State) ->
    {reply, catch do_clean_table(State#state.tables), State};
handle_call({clean, List}, _From, State) ->
    {reply, catch do_clean_table(List), State};
handle_call({diff_ref, Ref}, _, State) ->
    {reply, do_diff_ref(Ref, State), State};
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
    [{ok, _} = eredis_pool:transaction([[<<"LPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"del">>, <<"table">> => X})],
                                        [<<"DEL">>, X]]) || X <- List],
    do_clean_table(T);
do_clean_table([]) -> ok.

do_load_len() ->
    ets:new(?ETS, [named_table, public, {read_concurrency, true}]),
    {ok, BinLen} = eredis_pool:q([<<"LLEN">>, ?NOTICE]),
    Len = binary_to_integer(BinLen),
    ets:insert_new(?ETS, {ref, Len}), Len.

%%------------------------------------------------------------------------------
do_set_val([]) -> skip;
do_set_val(List) ->
    do_set_redis(List, []),
    do_set_ets(List), ok.

do_set_redis([{put, Table, Key, MKV} | T], Acc) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    do_set_redis(T, Acc ++
                 [[<<"LPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"put">>, <<"table">> => RedisTable})],
                  [<<"HMSET">>, RedisTable] ++ lists:flatten([[encode(CK), encode(CV)] || {CK, CV} <- MKV])]);
do_set_redis([{del, Table, Key} | T], Acc) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    do_set_redis(T, Acc ++
                 [[<<"LPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"del">>, <<"table">> => RedisTable})],
                  [<<"DEL">>, RedisTable]]);
do_set_redis([], Acc) -> {ok, _} = eredis_pool:transaction(Acc).

do_set_ets([{put, Table, Key, MKV} | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:insert(EtsTable, {Key, do_get_map(EtsTable, Key, MKV)}),
    do_set_ets(T);
do_set_ets([{del, Table, Key} | T]) ->
    ets:delete(?ETS_TABLE(Table), Key),
    do_set_ets(T);
do_set_ets([]) -> skip.

do_get_map(Name, Key, MKV) ->
    case ets:lookup(Name, Key) of
        [] -> maps:from_list(MKV);
        [{_Key, Map}] -> maps:merge(Map, maps:from_list(MKV))
    end.

%%------------------------------------------------------------------------------
do_rset_val(_Ref, []) -> skip;
do_rset_val(Ref, List) ->
    do_rset_redis(Ref, List, 0, [], []),
    do_set_ets(List), ok.

do_rset_redis(Ref, [{put, Table, Key, MKV} | T], Nth, Es, As) ->
    MKV1 = [[", '", encode(CK), "', '", encode(CV), "' "] || {CK, CV} <- MKV],
    MKV1 = [[", '", encode(CK), "', '", encode(CV), "' "] || {CK, CV} <- MKV],
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    {Nth1, Keys} = do_form_lua_key(MKV, Nth + 1, []),
    do_rset_redis(Ref, T, Nth1,
                  Es ++ [["redis.pcall('LPUSH', '", ?NOTICE, "', KEYS[", integer_to_binary(Nth + 1), "])"
                          " redis.pcall('HMSET', '", RedisTable, "'", Keys, ")"]],
                  As ++ [jsx:encode(#{<<"op">> => <<"put">>, <<"table">> => RedisTable})
                         | lists:flatten([[encode(CK), encode(CV)] || {CK, CV} <- MKV])]);
do_rset_redis(Ref, [{del, Table, Key} | T], Nth, Es, As) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    do_rset_redis(Ref, T, Nth + 1,
                  Es ++ ["redis.pcall('LPUSH', '", ?NOTICE, "', KEYS[", integer_to_binary(Nth + 1), "])"
                         " redis.pcall('DEL', '", RedisTable, "')"],
                  As ++ [jsx:encode(#{<<"op">> => <<"put">>, <<"table">> => RedisTable})]);
do_rset_redis(Ref, [], Nth, Es, As) ->
    {ok, <<"OK">>} = eredis_pool:q([<<"eval">>, ?SET_LUA(Ref, Es), Nth] ++ As).

do_form_lua_key([_ | T], Nth, Acc) ->
    V = [", KEYS[", integer_to_binary(Nth + 1), "], KEYS[", integer_to_binary(Nth + 2), "]"],
    do_form_lua_key(T, Nth + 2, [V | Acc]);
do_form_lua_key([], Nth, Acc) -> {Nth, Acc}.

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
                    ets:insert(?ETS, {ref, NLen}),
                    State#state{notice_len = NLen};
                NLen when NLen < Len andalso NLen > Len rem RLen ->
                    do_update_len(Len rem RLen, NLen, Tables),
                    ets:insert(?ETS, {ref, NLen}),
                    State#state{notice_len = NLen};
                NLen ->
                    ets:insert(?ETS, {ref, NLen}),
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
    #{<<"op">> := OP, <<"table">> := RedisTable} = jsx:decode(Val, [return_maps]),
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
do_diff_ref(RefLen, #state{tables = Tables} = State) ->
    {From, To} = do_from_to(RefLen, State),
    {ok, List} = eredis_pool:q([<<"LRANGE">>, ?NOTICE, From - 1, To - 1]),
    do_form_list(Tables, lists:usort(List), []).

do_from_to(RefLen, #state{notice_len = Len, reduce_len = RLen}) ->
    case RefLen < Len of
        true -> {RefLen, Len};
        false -> {RefLen rem RLen, Len}
    end.

do_form_list(Tables, [Val | T], Acc) ->
    #{<<"op">> := OP, <<"table">> := RedisTable} = jsx:decode(Val, [return_maps]),
    {Table, EtsKey} = do_parse_redis_table(RedisTable),
    case lists:member(Table, Tables) of
        true -> do_form_list(Tables, T, [{OP, ?ETS_TABLE(Table), EtsKey} | Acc]);
        false -> do_form_list(Tables, T, Acc)
    end;
do_form_list(_Tables, [], Acc) -> Acc.

