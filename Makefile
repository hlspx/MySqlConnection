CPP     = g++
RM      = rm

CPPFLAGS = -W -Wall 

LDLIBS = -lmariadbclient  

all: sample

sample: sample.cpp MySqlConnection.cpp MySqlConnection.h TmDateTime.cpp TmDateTime.h  
	$(CPP) $(CPPFLAGS) -o sample sample.cpp MySqlConnection.cpp TmDateTime.cpp $(LDLIBS)

clean: 
	rm -f sample 

