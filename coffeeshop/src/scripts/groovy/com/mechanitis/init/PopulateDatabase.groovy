package com.mechanitis.init

import com.mongodb.MongoClient
import com.mongodb.client.model.Indexes
import org.bson.Document

def mongoClient = new MongoClient()
def collection = mongoClient.getDatabase('Cafelito').getCollection('CoffeeShop')
// NOTE: This script drops the whole collection before reimporting it
collection.drop()

//NOTE: this requires the correct working directory (scripts) in the run configuration
def xmlSlurper = new XmlSlurper().parse(new File('resources/all-coffee-shops-2019.xml'))
xmlSlurper.node.findAll { it.tag.any { it.@k.text() == 'name' } }
               .each {
                   def coffeeShop = [openStreetMapId: it.@id.text(),
                                     location       : [coordinates: [it.@lon, it.@lat]*.text()*.toDouble(),
                                                       type       : 'Point']]
                   it.tag.findAll { isValidFieldName(it.@k.text()) }
                         .each {
                             coffeeShop.put(it.@k.text(), it.@v.text())
                         }
//                   println coffeeShop
                   collection.insertOne(new Document(coffeeShop))
               }

println "\nTotal imported: " + collection.countDocuments()

collection.createIndex(Indexes.geo2dsphere('location', '2dsphere'))

private static boolean isValidFieldName(fieldName) {
    !fieldName.contains('.') && !(fieldName == 'location')
}

//9373 - old data
//25051 - 2019 data