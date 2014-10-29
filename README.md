
An Attempt at a MapReduce reverse index program.  
================================================

Assumes multi-column table is implemented where column family acts as column name,
and data is in colunn qualifier (value is assumed "auxiliary" data of some sort)
and assumes the column to be inverted on is more-or-less a one-to-one mapping with
the row id.

For example, in this table:

    row  -  owns -  color - size  

    row1 -  a    -  blue  - med   

    row2 -  b    -  gren  - large

    row3 -       -  red   -

    row4 -  d    -        -

    row5 -  e    -  brown -

implemented in this way:

           - column   - column     - optional

    row id - family   - qualifier  - value

    row1   - color    - blue      - v2

    row1   - owns     - a         - v1

    row1   - size     - med       - v3

    row2   - color    - green     - v2

    row2   - owns     - b         - v1

    row2   - size     - large     - v3

    row3   - color    - red       - v2

    row4   - owns     - d         - v1

    row5   - color    - brown     - v2

    row5   - owns     - e         - v1

inverting on the column "owns" yields this new table

    a    - color      - blue      - v2

    a    - owned_by   - row1      - v1

    a    - size       - med       - v3

    b    - color      - green     - v2

    b    - color      - red       - v2

    b    - owned_by   - row2      - v1

    b    - size       - large     - v3

    d    - owned_by   - row4      - v1
 
    e    - color      - brown     - v2

    e    - owned_by   - row5      - v1

Note that row 3 is lost because there was no "owns" record.


To Build, use maven
-------------------

    test compilation:   mvn test-compile

    build:              mvn package

This creates a jar file in ./target


To Run
------

examine and modifity where necessary ./run_mrRevInd

. ~/accumulo-env.sh     # setup accumulo environment

./run_mrRevInd  source-table  output-table  family-to-be-reversed   reverse-name







