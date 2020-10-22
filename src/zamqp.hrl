-ifndef(OTP_RELEASE).
-define(WITH_STACKTRACE(T, R, S), T:R -> S = erlang:get_stacktrace(), ).
-else.
-define(WITH_STACKTRACE(T, R, S), T:R:S ->).
-endif.

-define(DEBUG(M), zamqp_log:log(debug, M)).
-define(DEBUG(M, Meta), zamqp_log:log(debug, M, Meta)).
-define(INFO(M), zamqp_log:log(info, M)).
-define(INFO(M,Meta), zamqp_log:log(info, M, Meta)).
-define(WARNING(M), zamqp_log:log(warning, M)).
-define(WARNING(M,Meta), zamqp_log:log(warning, M, Meta)).
-define(ERROR(M), zamqp_log:log(error, M)).
-define(ERROR(M,Meta), zamqp_log:log(error, M, Meta)).
-define(UNEXPECTED(Type, Content),
        ?WARNING("Unexpected ~p: ~p", [Type, Content])).


