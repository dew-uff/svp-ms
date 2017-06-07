<results>
 { 
 for $pe in doc('xmark.xml')/site/people/person
 for $it in doc('xmark.xml')/site/regions/samerica/item
 let $income := $pe/profile/@income
 where $income > 90000
 and $pe/profile/interest/@category = $it/incategory/@category
 group by $income
 order by count($it/VP) descending
 return 
 <record>
     <person>{$pe/name}</person>
     <income>{$income}</income>
     <sum_quantity>{sum($it/quantity)}</sum_quantity>
     <count_item>{count($it)}</count_item>
     <avg_quantity>{avg($it/quantity)}</avg_quantity>
     <item>{$it/name}</item>
     <count_category>{count($it/incategory)}</count_category>
 </record>
 } </results>