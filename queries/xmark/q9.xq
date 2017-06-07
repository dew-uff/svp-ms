<results>
 {
 for $it in doc('xmark.xml')/site/regions/africa/item
 for $co in doc('xmark.xml')/site/closed_auctions/closed_auction
 where $co/itemref/@item = $it/@id
 and $it/payment = "Cash"
 return
 <record>
    <price>{$co/price}</price>
    <date>{$co/date}</date>
    <quantity>{$co/quantity}</quantity>
    <type>{$co/type}</type>
    <payment>{$it/payment}</payment>
    <location>{$it/location}</location>
    <from>{$it/from}</from>
    <to>{$it/to}</to>
 </record>
 } </results>