mvn install -DskipTests=true
cp storm-core/target/storm-core-0.11.0-SNAPSHOT.jar storm-dist/binary/target/apache-storm-0.11.0-SNAPSHOT/lib/
cp examples/storm-starter/target/storm-starter-0.11.0-SNAPSHOT.jar storm-dist/binary/target/apache-storm-0.11.0-SNAPSHOT/examples/storm-starter/storm-starter-topologies-0.11.0-SNAPSHOT.jar

echo "done!"
say "compiling complete!"
