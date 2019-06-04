# spark-programming-hdfs

In this project I will be working with a dataset containing a collection of Emails exchanged by employees of the Enron corporation in the late 1990s and early 2000s. This dataset became a public record following a high-profile investigation by the US Securities and Exchange Commission (SEC), which resulted in several Enron’s top executives being prosecuted for fraud, and the company itself going bankrupt (see https://en.wikipedia.org/wiki/Enron_scandal for more details).

The dataset is structured as a Hadoop Sequence File comprising a collection of binary objects each of which representing a single Email message.

Using the Spark programming in Python I will analyse basic properties of the network induced by the full Enron Email dataset.

### Observations

Lineage graph when count() is called on the RDD returned by the extract_email_network function-

![Alt text](res/lineage_graph.png?raw=true "Title")

It can be seen from the lineage graph(DAG) above that all the transformations induce a narrow dependency.There is no wide dependency in the above diagram as there is no shuffling of Data.

The number of connections originating at or attracted by nodes in a scale-free network follows an 80/20 rule, that is, roughly 20% of the nodes are either origins or destinations of 80% of all edges in the network. (These 20% of nodes are, in fact, the hubs.) Using the code to compute the total weighted degree of all edges originating at (respectively attracted by) the 20% highest out-degree (respectively,in-degree) nodes in the network slice in the range 1/May/2000 to 30/April/2001.

![Alt text](res/out_degree_piechart.png?raw=true "Title")
The ratio is 72/28 for out-degree. Obtained by counting the number of nodes for which out-degree is not equal to zero i.e 3321 nodes from a total of 11991 nodes

![Alt text](res/in_degree_piechart.png?raw=true "Title")
The ratio is 2/98 for in-degree. Obtained by counting the number of nodes for which in-degree is not equal to zero i.e 11746 nodes from a total of 11991 nodes.

The following line chart shows the relationship between the maximum node degree-Kmax vs the total number of nodes in the network for each sub-slice starting from 1 month to 12 months. The value of Kmax is obtained by calling rdd.first() action on the resulting rdd of outdegree or indegree functions and total nodes are calculated using count() action on the same rdd. It can be seen from the chart below that Kmax is direcly proportional to the number of nodes.
![Alt text](res/power_law.png?raw=true "Title")

Probability distributions-
For outdegree distribution- calculated the out-degree-dist for the selected slice of 12 months and plotted the log of distribution on y-axis vs the log of the weighted outdegree of the node on the x-axis. It seems to be a linear line with negative slope.
![Alt text](res/out_degree_dist.png?raw=true "Title")

For in-degree distribution- calculated the in-degree-dist for the selected slice of 12 months and plotted the log of distribution on y-axis vs the log of the weighted indegree of the node on the x-axis. This also seems to be a linear line with negative slope.
![Alt text](res/in_degree_dist.png?raw=true "Title")
