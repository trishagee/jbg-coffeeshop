package com.mechanitis.init

import com.mongodb.MongoClient
import com.mongodb.client.model.Indexes
import org.bson.Document

def mongoClient = new MongoClient()
def collection = mongoClient.getDatabase('Cafelito').getCollection('CoffeeShop')
// NOTE: This script drops the whole collection before reimporting it
collection.drop()

//NOTE: this requires the correct working directory (scripts) in the run configuration
def xmlSlurper = new XmlSlurper().parse(new File('resources/all-coffee-shops.xml'))

xmlSlurper.node.each {
    def coffeeShop = [openStreetMapId: it.@id.text(),
                      location       : [coordinates: [it.@lon, it.@lat]*.text()*.toDouble(),
                                        type       : 'Point']]
    it.tag.each {
        def fieldName = it.@k.text()
        if (isValidFieldName(fieldName)) {
            coffeeShop.put(fieldName, it.@v.text())
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
    !fieldName.contains('.') && !(fieldName == 'location')
}
