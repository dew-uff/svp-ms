<results>
{
 for $ca in doc('xmark.xml')/site/closed_auctions/closed_auction
 for $pe in doc('xmark.xml')/site/people/person
 for $ei in doc('xmark.xml')/site/regions/europe/item
 let $peid := $pe/@id
 where $ca/buyer/@person = $pe/@id 
 and $ca/itemref/@item = $ei/@id
 return
 <record>
       <p_id>{$peid}</p_id>
       <p_name>{$pe/name}</p_name>
       <item>{$ei/name}</item>
 </record>
 } </results>