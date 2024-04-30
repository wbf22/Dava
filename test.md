1 2 3 40 50 60 100 110 300 320 322 323 325 330 360 400


    100
     |
   2-'-325
   |     |
 2-'-4 7-'-3


What we really need is a data structure which does this:
- fast insertion
- fast access (by any piece of data)
- reasonable deletion




One file for indices (or for each column) would be preferable (x300 speed for insert).
- this is only better if you don't seek/jump to different positions in the file
