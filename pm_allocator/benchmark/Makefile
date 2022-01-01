
#Allocator:= -DPMDK
Allocator:= $(alloc)

REC:= $(recover)


all: Allocators liballocator.so
INCLUDES:= -I./ -I../allocator/makalu/include -I../allocator/nvm_malloc/src -I../allocator/pmdk/src/include
LIBS_DIR:= -L./ -L../allocator/pmdk/src/nondebug
LIB:=  -lallocator -lpthread -ljemalloc
CXXFLAGS:= -O3 -g -std=c++17  $(LIBS_DIR) $(INCLUDES) -Wl,-rpath=./ $(REC) 

Allocators:
	make -C ../allocator/makalu 
	cp ../allocator/makalu/lib/* ./
	make -C ../allocator/nvm_malloc
	cp ../allocator/nvm_malloc/*.so ./
	sudo chmod -R 777 ../allocator/pmdk/utils
	make -C ../allocator/pmdk/src
	cp ../allocator/pmdk/src/nondebug/*.so* ./

liballocator.so: Allocator.cpp Allocator.hpp
	$(CXX) $(CXXFLAGS) $(LIBS_DIR) $(INCLUDES) $(Allocator) -fPIC -shared Allocator.cpp -o $@ -lmakalu -lnvmmalloc -lpmemobj
	
clean:
	rm -f *.o *.so *.a
