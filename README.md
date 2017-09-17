# Implement FIM in MapReduce framework

FIM(Frequent Itemsets Mining)

## Introduction

This algorithm call **One-Phase**

And I implement this Algorithm from this paper

> *O. Yahya, O. Hegazy and E. Ezat, ”An efficient implementation of Apriori algorithm based on Hadoop-MapReduce model,” Inter- national Journal of Reviews in Computing, vol. 12, pp. 59V67, 12 2012*

This algorithm is using an easy way to get frequent itemsets

### Concept

**One-Phase** algorithm let every **Transaction** decompose into every subsets

Example:
	
	TID:1  {a,b,c}

Subsets:

	{a,b,c,ab,ac,bc,abc}

### MapReduce framework

#### Map part

To generate every subsets in each transcation list and output <subset,1> 

**Input:**

	Itemset from transcation list

**Decompose** part referencer [here](http://www.geeksforgeeks.org/finding-all-subsets-of-a-given-set-in-java/ "Title")

**Output:**

	Key:Every subsets from transcation list

	Value:Itemset count 1

#### Reduce part

According each Key to sum the value

If value equal or bigger than **Minsupport** output frequent itemsets


