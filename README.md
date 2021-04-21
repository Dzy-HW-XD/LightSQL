# LightSQL

## Operator: Projection, Join, Selection, Scan and Aliases
## Goal
In this Project,I implement a lightweight database management system
called LightDB without ORDER BY, DISTINCT operator.

## Description
In the following paragraphs, i will focus on explaining my logic for extracting 
join conditions from the WHERE clause.

The function of extracting join conditions from WHERE clause is implemented by
getJoinConditionList method in sqlParser Class. In this method, i firstly obtain the
whole WHERE clause using getWhere() method and then Override ExpressionDeParser method.

ExpressionDeParser method  can sequentially parse the components in WHERE clause, so the point is to 
override its visitor mode and get the Join condition when the corresponding VISIT function is excited.
In my project, i override five visitor functions, visit(EqualsTo equalsTo),visit(GreaterThan greaterThan),
visit(GreaterThanEquals greaterThanEquals),visit(MinorThanEquals minorThanEquals) and visit(MinorThan minorThan).
So the idea is to determine if the right side of Expression is a Column class. If right side of expression belongs to 
Column class, then output the whole expression and label it as join condition expression.

```
@Override
                    public void visit(EqualsTo equalsTo) {
                        super.visit(equalsTo);
                        if (equalsTo.getRightExpression() instanceof Column) {
                            stack.push(equalsTo);
                        }
                    }
```

Here is the code about EqualsTo class visitor. When the right side of equalsTo is equal to Column
class, the whole equalsTo is considered as Join Condition. 
```
For example, SELECT * FROM Sailors,Boats
                                     WHERE Sailors.A = Boats.E
                                     AND Sailors.A < 3
                                     AND Boats.F = 1
```
Expression Sailors.A = Boats.E will be pushed into stack and labelled as Join Conditon, Sailors.A < 3 and Boats.F = 1
will be considered as Select Condition.

Moreover, GreaterThan,GreaterThanEquals,MinorThanEquals,MinorThan have same principle as EqualsTo does.
```
For example, SELECT * FROM Sailors S,Reserves R 
		    WHERE S.A = R.G 
	                    AND S.B<R.H;
```
Expression S.A = R.G and S.B<R.H will be labelled as Join Condition in this sql case.

## Query Plan

For example, if we have SELECT * FROM R, S, T WHERE R.A = 1 AND R.B = S.C
AND T.G < 5 AND T.G = S.H, the above approach would give the following query plan:

![image](https://github.com/Dzy-HW-XD/LightSQL/blob/main/query%20plan.png)

## Classes Description:

* databaseCatalog() is used to get Information of Database such as Path, Attribute information.
* JoinOperator() is a child of Operator, which is used to accomplish cross product and conditional filter.
* Operator() is a abstract class, which is the father class of other Operator, containing getNexTuple,dump and reset function.
* ProjectOperator() is used to accomplish projection.
* ScanOperator() is child of Operator(), which is used to scan and return all tuples of data file.
* SelectOperator() is child of Operator(), which is used to select and return those tuples that meet the criteria.
* SimpleEvaluate() is a class that used to test the ExpressionDeParser().
* sqlParser() is an important class in my project which is used to get List of Select and Join Condition Expression from sql.
* sqlProcess() is a class that is used to get final tuple results.
* TableToAppropriateExpression() class is an important class in my project which is used to map appropriate expression to corresponding table name.
	eg.
 	            sql: SELECT  R.G, s.C, s2.A,b0.D, s2.B, b0.E, b0.F, b1.D, b1.E, b1.F,R.H 
	            FROM Reserves R,Sailors s,Sailors s2,Boats b0,Boats b1 
			WHERE R.H >= 102 
			      and s.B=100 
			      and s2.A = 6 
		                      and R.H < 104 
			      and s.A = R.G 
			      and s2.A<s2.B 
			      and b1.D<103;

	            R: SELECT * FROM Reserves WHERE Reserves.H < 104 AND Reserves.H >= 102 
	            s: SELECT * FROM Sailors WHERE Sailors.B = 100 
	            b0: null
	            s2: SELECT * FROM Sailors WHERE Sailors.A = 6 
	            b1: SELECT * FROM Boats WHERE Boats.D < 103 
* Tuple() is a class that define tuple, one tuple is defined by one HashTable and One List.

## Demo

```
Boats
D,E,F
101,2,3
102,3,4
104,104,2
103,1,1
107,2,8
```
```
Reserves:
G,H
1,101
1,102
1,103
2,101
3,102
4,104
```

```
Sailors:
A,B,C
1,200,50
2,200,200
3,100,105
4,100,50
5,100,500
6,300,400
```

>>`mvn clean compile assembly:single`#Compile code and produce a runnable .jar file<br>
```
$ java -jar target/lightdb-1.0.0-jar-with-dependencies.jar
Usage: LightDB database_dir input_file output_file
```
>>`java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query1.sql samples/output/query1.csv` read sql from samples/input/query1.sql and print results into samples/output/query1.csv<br>

```
samples/input/query1.sql: 
SELECT  R.G, s.C, s2.A,b0.D, s2.B, b0.E, b0.F, b1.D, b1.E, b1.F,R.H FROM Reserves R,Sailors s,Sailors s2,Boats b0,Boats b1 WHERE R.H >= 102 and s.B=100 and s2.A = 6 and R.H < 104 and s.A = R.G and s2.A<s2.B and b1.D<103;
```
```
samples/output/query1.csv
3 105 6 101 300 2 3 101 2 3 102 
3 105 6 101 300 2 3 102 3 4 102 
3 105 6 102 300 3 4 101 2 3 102 
3 105 6 102 300 3 4 102 3 4 102 
3 105 6 104 300 104 2 101 2 3 102 
3 105 6 104 300 104 2 102 3 4 102 
3 105 6 103 300 1 1 101 2 3 102 
3 105 6 103 300 1 1 102 3 4 102 
3 105 6 107 300 2 8 101 2 3 102 
3 105 6 107 300 2 8 102 3 4 102 
```


## Reference
https://github.com/JSQLParser/JSqlParser 
https://javadoc.io/doc/com.github.jsqlparser/jsqlparser/latest/index.html





 





