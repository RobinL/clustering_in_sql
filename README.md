# clustering_in_sql

Our initial implementation came from the paper "In-database connected component analysis" by Harald Bögeholz, Michael Brand, and Radu-Alexandru Todor (https://arxiv.org/pdf/1802.09478.pdf).

> 'begin by choosing for each vertex (node) a representatative by picking the vertex > with the minimum id amongst itself and its neighbours'
>
> i.e. attach neighbours to nodes and find the minumum
>
> Note that, since the edges always have the lower id on the left hand side we only > need to join on unique_id_r, and pick unique_id_lThat is to say, we only bother to > find neighbours that are smaller than the node
>
> i.e if we want to get the neighbours of node D, we don't need to get both D->C and > D->E, we only need to bother getting D->C, because we know in advance this will have > a lower minimum that D->E


The problem we habe in Splink at the moment is that we have a large number of iterations - up to about 42.

[In-database connected component analysis](https://arxiv.org/pdf/1802.09478) proposes a new algo called randomzied contraction

> The key difference is that the simple breadth-first approach can have a linear number of iterations in the worst case, while the randomized contraction algorithm achieves a logarithmic expected number of iterations through clever use of randomization

So a ‘chain’ type graph of A -> B -> C -> D -> E takes 4 iterations with our current approach.   More generally, if there are n links in the chain, it takes n iterations

With the new appraoch it’s much faster.  For example, on 10,000 links in the chain, the new algo converges in 15 iterations

This is a potential improvement to Splink.