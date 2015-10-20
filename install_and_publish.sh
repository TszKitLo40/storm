mvn install -DskipTests=true
cp storm-core/target/storm-core-0.11.0-SNAPSHOT.jar storm-dist/binary/target/apache-storm-0.11.0-SNAPSHOT/lib/
echo "done!"
eject -r
eject -t
