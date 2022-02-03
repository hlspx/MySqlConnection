#include <iostream>
#include "MySqlConnection.h"

using namespace Kiff;

int main()
{
	const char *sql =
		"DROP table if exists person; "
		"create table person (name varchar(256), age int unsigned, weight double) ENGINE=InnoDB  DEFAULT CHARSET=utf8; "
		"insert into person(name,age,weight) values('Mary',25,73.8);";

	MySqlConnection conn("server=localhost;User Id=test;Password=12345;character set=utf8;database=conntest");
	conn.ExecuteNonQuery(sql);

	const char *name = "Bob";
	int age = 33;
	double weigh = 88.4;
	conn.ExecuteNonQuery("insert into person(name,age,weight) values(?,?,?)", name, age, weigh);

	int minage = 30;
	std::string nm;
	double wg;

	MySqlDataReader *rd = conn.ExecuteReader("select name,weight from person where age > ?", minage);
	while (rd->Read())
	{
		rd->GetValues(nm, wg);
		std::cout << nm << " " << wg << std::endl;
	}
	delete rd;
}