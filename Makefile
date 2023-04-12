.PHONY: package
package:
	mvn -v && mvn clean package -DskipTests

dist: package
	rm -rf dist/marayarn
	mkdir -p dist/marayarn/plugins dist/marayarn/bin
	cp marayarn-am/target/marayarn-am-*-with-dependencies.jar dist/marayarn/marayarn-am.jar
	cp plugins/ck-sinker/target/plugin-ck-sinker-*-with-dependencies.jar dist/marayarn/plugins/
	cp plugins/logstash/target/plugin-logstash-*.jar dist/marayarn/plugins/
	cp marayarn-cli/release/marayarn dist/marayarn/bin/
	cp marayarn-cli/release/log4j.properties dist/marayarn/bin/
	cp marayarn-cli/target/marayarn-cli-*.jar dist/marayarn/bin/
	cp -r marayarn-cli/target/lib dist/marayarn/bin/

