package com.mechanitis.init

import com.mongodb.MongoClient
import com.mongodb.client.model.Indexes
import org.bson.Document

def mongoClient = new MongoClient()
def collection = mongoClient.getDatabase('Cafelito').getCollection('CoffeeShop')
collection.drop()

println new File('resources/all-coffee-shops.xml').getAbsolutePath()

def xmlSlurper = new XmlSlurper().parse(new File('resources/all-coffee-shops.xml'))

xmlSlurper.node.each { child ->
    def coffeeShop = [openStreetMapId: child.@id.text(),
                      location       : [coordinates: [child.@lon, child.@lat]*.text()*.toDouble(),
                                        type       : 'Point']]
    child.tag.each { theNode ->
        def fieldName = theNode.@k.text()
        if (isValidFieldName(fieldName)) {
            coffeeShop.put(fieldName, theNode.@v.text())
        }
    }
    if (coffeeShop.name) {
        println coffeeShop
        collection.insertOne(new Document(coffeeShop))
    }
}

println "\nTotal imported: "+collection.countDocuments()

collection.createIndex(Indexes.geo2dsphere('location', '2dsphere'))

private static boolean isValidFieldName(fieldName) {
    !fieldName.contains('.') && fieldName != 'location'
}
