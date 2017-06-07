<results>
 {
 for $pe in doc('xmark.xml')/site/people/person
 for $cat in doc('xmark.xml')/site/categories/category
 let $name := $pe/name
 where count($pe/profile/interest) > 3
 and $pe/profile/interest/@category = $cat/@id
 order by $name/VP
 return
 <record>
     <person>{$name}</person>
     <count_item>{count($cat)}</count_item>
     <category>{$cat/name}</category>
 </record>
} </results>