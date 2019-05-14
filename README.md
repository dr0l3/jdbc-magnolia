# jdbc-magnolia

This repo contains code for "half an ORM":D

What is meant by that is that the code that is capable of

- Creating tables for arbitrary hierarchies of sealed traits and case classes
- Inserting values for arbitrary hierarchies of sealed traits and case classes
- Find/select values for arbitrary hierarchies of sealed traits and case classes

See the tests for examples of the code in use.

The basic idea is to take values/types and compile them down to sql that is then executed in parallel using cats IO.

Lists are supported, but options, eithers, maps or other datastructures are not. Adding support for delete and update as well beforementioned datastructures should be possible.

The code is prototype code and would need a bit of love before it is fit for "serious business" :)
