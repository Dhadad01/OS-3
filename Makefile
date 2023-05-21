AUXILIARY_SOURCES := worker.cpp job.cpp pdebug.cpp
AUXILIARY_HEADERS := ${AUXILIARY_SOURCES:.cpp=.hpp}
SOURCES := MapReduceFramework.cpp $(AUXILIARY_SOURCES)
LIBRARY := libMapReduceFramework.a
OBJECTS := ${SOURCES:.cpp=.o}
TARGET_SOURCES := test1.cpp
TARGET_OBJECTS := ${TARGET_SOURCES:.cpp=.o}
TARGET := target

CXX := g++
AR := ar
RANLIB := ranlib
CXXFLAGS := -Werror -Wextra -Wall -std=c++11 -pedantic
LDFLAGS := -pthread
CLANG_FORMAT_FLAGS := --style=file:.clang-format -i
FILES_TO_SUBMIT := Makefile README $(AUXILIARY_SOURCES) $(AUXILIARY_HEADERS) MapReduceFramework.cpp
SUBMISION_NAME := ex3
SUBMISION_FILE := ${SUBMISION_NAME:=.tar}

.PHONY: all clean format tar

$(LIBRARY): $(OBJECTS)
	$(AR) rcs $@  $^
	$(RANLIB) $@

debug: CXXFLAGS += -g -O0 -DDEBUG
debug: all

all: $(TARGET)

$(TARGET): $(TARGET_OBJECTS) $(LIBRARY)
	$(CXX) $(LDFLAGS) $^ -o $(TARGET)

%.o : %.cpp
	$(CXX) -c $(CXXFLAGS) $(CPPFLAGS) $< -o $@

clean:
	rm -rf $(OBJECTS) $(TARGET_OBJECTS) $(TARGET) $(LIBRARY) $(SUBMISION_FILE)

format:
	clang-format $(CLANG_FORMAT_FLAGS) $(SOURCES) $(AUXILIARY_HEADERS)

tar: $(FILES_TO_SUBMIT)
	tar cf $(SUBMISION_FILE) $(FILES_TO_SUBMIT)

