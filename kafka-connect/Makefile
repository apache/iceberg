GRADLE := ../gradlew
GRADLE_FLAGS := -x test -x integrationTest

.PHONY: dist clean

dist:
	$(GRADLE) :iceberg-kafka-connect:iceberg-kafka-connect-runtime:installDist $(GRADLE_FLAGS)

clean:
	$(GRADLE) :iceberg-kafka-connect:iceberg-kafka-connect-runtime:clean
