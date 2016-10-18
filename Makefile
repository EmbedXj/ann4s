all:
	g++ -Iannoy/src -O3 -march=native -ffast-math -o src/main/resources/darwin/libannoy.dylib -shared -fPIC src/main/cpp/annoyjava.cpp
	rm -rf db
	rm -f logs
	sbt "runMain annoy.AnnoyIndexTest" > logs 2>&1
	grep ^s logs | sed s/^..// > scala.logs
	grep ^n logs | sed s/^..// > native.logs
	vimdiff scala.logs native.logs
