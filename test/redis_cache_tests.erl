-module(redis_cache_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() -> application:start(redis_cache), redis_cache_copy:start_link([test1]) end).
-define(Clearnup, fun(_) -> gen_server:stop(redis_cache_copy), application:stop(redis_cache) end).

basic_test_() ->
    {inorder,
     {setup, ?Setup, ?Clearnup,
      [{"redis",
         fun() ->
                 ?assertEqual(ok, element(1, eredis_pool:q([<<"INFO">>])))
         end},
      {"put_val",
         fun() ->
                 ?assertEqual(ok, redis_cache:put_val(test1, key1, ckey1, cval1)),
                 ?assertEqual(cval1, redis_cache:get_val(test1, key1, ckey1)),
                 ?assertEqual(ok, redis_cache:reload()),
                 ?assertEqual(1, length(redis_cache:get_table_vals(test1))),
                 ?assertEqual(1, length(redis_cache:get_redis_vals(test1))),
                 ?assertEqual(cval1, redis_cache:get_val(test1, key1, ckey1)),
                 ?assertEqual(ok, redis_cache:put_val(test1, key1, ckey1, cval2)),
                 ?assertEqual(cval2, redis_cache:get_val(test1, key1, ckey1))
         end},
       {"syn_val",
         fun() ->
                 ?assertEqual(cval2, redis_cache:get_val(test1, key1, ckey1)),
                 ?assertEqual(ok, redis_cache:put_val(test1, key1, ckey1, cval3)),
                 timer:sleep(500),
                 ?assertEqual(cval3, redis_cache_copy:get_val(test1, key1, ckey1)),
                 ?assertEqual(ok, redis_cache:del_val(test1, key1)),
                 timer:sleep(500),
                 ?assertEqual(null, redis_cache_copy:get_val(test1, key1, ckey1))
         end},
       {"lock",
         fun() ->
                 ?assertEqual(0, length(redis_cache:get_locks())),
                 ?assertEqual(ok, redis_cache:lock()),
                 ?assertEqual(1, length(redis_cache:get_locks())),
                 ?assertEqual(ok, redis_cache:put_val(test1, key1, ckey1, cval4)),
                 ?assertEqual(0, length(redis_cache:get_locks())),
                 ?assertEqual(ok, redis_cache:lock()),
                 ?assertEqual(1, length(redis_cache:get_locks())),
                 ?assertEqual(ok, redis_cache:unlock()),
                 ?assertEqual(0, length(redis_cache:get_locks())),
                 ?assertEqual(ok, redis_cache:lock()),
                 ?assertEqual(ok, redis_cache_copy:put_val(test1, key1, ckey1, cval5)),
                 ?assert(ok =/= redis_cache:put_val(test1, key1, ckey1, cval6)),
                 timer:sleep(500),
                 ?assertEqual(cval5, redis_cache:get_val(test1, key1, ckey1)),
                 PID = spawn(fun() -> ok = redis_cache:lock(), timer:sleep(5000) end),
                 timer:sleep(500),
                 ?assertEqual(1, length(redis_cache:get_locks())),
                 exit(PID, kill),
                 timer:sleep(600),
                 ?assertEqual(0, length(redis_cache:get_locks()))
         end},
       {"reduce",
         fun() ->
                 ?assertEqual(ok, redis_cache:purge()),
                 timer:sleep(800),
                 redis_cache:put_val([{test1, X, [{ckey1, cval4}]} || X <- lists:seq(1, 750)]),
                 ?assertEqual(750, redis_cache:get_redis_notice_len()),
                 timer:sleep(1000),
                 ?assertEqual(750, redis_cache:get_cache_notice_len()),
                 redis_cache:put_val([{test1, X, [{ckey1, cval4}]} || X <- lists:seq(751, 1000)]),
                 timer:sleep(800),
                 ?assertEqual(250, redis_cache:get_redis_notice_len()),
                 ?assertEqual(ok, redis_cache:purge())
         end}
      ]}
    }.

