# Always run the fat jar so the benchmark includes realistic JVM startup cost.
# ZGC generational mode: concurrent collection, sub-ms pauses under steady load.
# -Xms == -Xmx: pre-commit full heap at startup so no resize GC during cycles.

JAR     := build/libs/priority-service-jvm.jar
JAVA    ?= java
JAVA_OPTS := \
    -XX:+UseZGC \
    -XX:+ZGenerational \
    -Xms2g \
    -Xmx4g \
    -XX:ConcGCThreads=2 \
    -XX:ParallelGCThreads=4 \
    -server

# Phase 2 goroutine-pool equivalent: override with PS_NUM_WORKERS=N
# Phase 1 parallel stream threads: override with PS_P1_PARALLELISM=N
# GC: override with PS_GC_MODE=zgc|g1|serial for A/B comparison

.PHONY: run build test clean bench

build:
	./gradlew shadowJar

run: build
	$(JAVA) $(JAVA_OPTS) -jar $(JAR)

bench: build
	$(JAVA) $(JAVA_OPTS) -jar $(JAR)

# G1GC variant — compare GC pause histograms vs ZGC
bench-g1: build
	$(JAVA) \
	    -XX:+UseG1GC \
	    -XX:MaxGCPauseMillis=50 \
	    -Xms14g -Xmx14g -server \
	    -jar $(JAR)

# Serial GC baseline — worst case, single-threaded collection
bench-serial: build
	$(JAVA) -XX:+UseSerialGC -Xms14g -Xmx14g -server -jar $(JAR)

test:
	./gradlew test

clean:
	./gradlew clean
