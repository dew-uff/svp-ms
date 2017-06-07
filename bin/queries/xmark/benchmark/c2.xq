<results> {
 for $auction in doc('xmark.xml')/site/open_auctions/open_auction
 let $bidder := $auction/bidder
 return
    <record>
       {$bidder[1]/increase}
    </record>
} </results>
