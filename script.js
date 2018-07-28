const MongoClient=require('mongodb').MongoClient
const async=require('async')
const customerAddress=require('./m3-customer-address-data.json')
const customer=require('./m3-customer-data.json')
const url='mongodb://localhost:27017/demo'

let batchSize=parseInt(process.argv[2])
if(1000%batchSize!==0){
    console.log('batch size must divide 1000 evenly')
    process.exit(1)
}

console.time('totalTime')
console.time('tasksArray')
let tasks=[]
for(let i=0;i<1000/batchSize;i++){
    tasks.push(()=>{
        let docs=[]
        console.log(`Processing docs from ${i*batchSize} to ${i*batchSize+batchSize-1}`)
        for(let j=i*batchSize;j<i*batchSize+batchSize;j++){
            let newDoc=customer[j]
            docs.push(Object.assign(newDoc,customerAddress[j]))
        }
        MongoClient.connect(url,{useNewUrlParser:true},(error,db)=>{
            if(error) throw error
            let collection=db.db().collection('bitcoinUser')
            collection.insert(docs,(error,result)=>{
                if(error) throw error
                else console.log(`Transfer successful from ${i*batchSize} to ${i*batchSize+batchSize-1}`)
            })
            db.close()
        })
    })
}
console.timeEnd('tasksArray')

console.time('database')
async.parallel(tasks,(error,result)=>{
    if(error) throw error
    console.log(result)
})
console.timeEnd('database')
console.timeEnd('totalTime')