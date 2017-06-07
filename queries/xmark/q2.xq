<results>
{
 for $ca in doc('xmark.xml')/site/closed_auctions/closed_auction
 for $pe in doc('xmark.xml')/site/people/person
 for $ei in doc('xmark.xml')/site/regions/europe/item
 let $peid := $pe/@id
 let $name := $pe/name
 where $ca/buyer/@person = $pe/@id
 and $ca/itemref/@item = $ei/@id
 group by $peid, $name
 order by $peid/VP, $name/VP
 return
 <record>
       <p_id>{$peid}</p_id>
       <p_name>{$name}</p_name>
       <count_item>{count($ei)}</count_item>
 </record>
 } </results>