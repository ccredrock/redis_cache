ERL=erl
REBAR=./rebar
BEAMDIR=./deps/*/ebin ./ebin

.PHONY: deps doc test
all: clean deps compile xref

clean:
	@$(REBAR) clean

deps:
	@$(REBAR) get-deps
	@$(REBAR) update-deps

compile:
	@$(REBAR) compile

xref:
	@$(REBAR) xref skip_deps=true

doc:
	@$(REBAR) skip_deps=true doc

test:
	@rm -rf .eunit
	@mkdir -p .eunit
	@ERL_FLAGS="-config test.config" $(REBAR) skip_deps=true eunit

run:
	@$(ERL) -pa ebin -pa deps/*/ebin -config test.config -s redis_sync start

