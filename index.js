const express = require('express');
const app = express();
const cors = require('cors');
const port = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

const { MongoClient, ServerApiVersion } = require('mongodb');
const uri = `mongodb+srv://db_user_read:zHLuO45zk1upaRmp@cluster0.aaflc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0`;

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

async function run() {
  try {
    // Connect the client to the server (optional starting in v4.7)
    await client.connect();
    const database = client.db("RQ_Analytics");

    const customers = await database.collection('shopifyCustomers')
    const products = await database.collection('shopifyProducts')
    const orders = await database.collection('shopifyOrders')

app.get('/customers', async(req,res)=>{
  const allCustomer= await customers.find().toArray()
  res.send(allCustomer);
})
app.get('/products', async(req,res)=>{
  const allProducts= await products.find().toArray()
  res.send(allProducts);
})
app.get('/orders', async(req,res)=>{
  const allOrders= await orders.find().toArray()
  res.send(allOrders);
})

// Daily sales
app.get('/dailysale', async(req,res)=>{
  const dailySales = await orders.aggregate([
    {
      $addFields: {
        created_at_date: { $toDate: "$created_at" } 
      }
    },
    {
      $group: {
        _id: { $dateToString: { format: "%Y-%m-%d", date: "$created_at_date" } },
        totalSales: { $sum: { $toDouble: "$total_price_set.shop_money.amount" } }
      }
    },
    { $sort: { _id: 1 } }
  ]).toArray();
  res.send(dailySales)

})
// Monthly sales
app.get('/monthlysale', async(req,res)=>{
  const monthlySales = await orders.aggregate([
    {
      $addFields: {
        created_at_date: { $toDate: "$created_at" }
      }
    },
    {
      $group: {
        _id: { $dateToString: { format: "%Y-%m", date: "$created_at_date" } },
        totalSales: { $sum: { $toDouble: "$total_price_set.shop_money.amount" } }
      }
    },
    { $sort: { _id: 1 } }
  ]).toArray();
  res.send(monthlySales)
  
})

// Yearly sales
app.get('/yearly', async(req,res)=>{
  const YearlySales = await orders.aggregate([
    {
      $addFields: {
        created_at_date: { $toDate: "$created_at" }
      }
    },
    {
      $group: {
        _id: { $dateToString: { format: "%Y", date: "$created_at_date" } },
        totalSales: { $sum: { $toDouble: "$total_price_set.shop_money.amount" } }
      }
    },
    { $sort: { _id: 1 } }
  ]).toArray();
  res.send(YearlySales)
})


 // Quarterly Sales
 app.get('/quarter', async(req,res)=>{
  const quarterlySales = await orders.aggregate([
    {
      $addFields: {
        created_at_date: { $toDate: "$created_at" }
      }
    },
    {
      $group: {
        _id: {
          year: { $year: "$created_at_date" },
          quarter: {
            $switch: {
              branches: [
                { case: { $lte: [{ $month: "$created_at_date" }, 3] }, then: "Q1" },
                { case: { $lte: [{ $month: "$created_at_date" }, 6] }, then: "Q2" },
                { case: { $lte: [{ $month: "$created_at_date" }, 9] }, then: "Q3" }
              ],
              default: "Q4"
            }
          }
        },
        totalSales: { $sum: { $toDouble: "$total_price_set.shop_money.amount" } }
      }
    },
    {
      $project: {
        _id: { $concat: [{ $toString: "$_id.year" }, "-", "$_id.quarter"] },
        totalSales: 1
      }
    },
    { $sort: { _id: 1 } }
  ]).toArray();
  res.send(quarterlySales)
 })

 app.get('/newcustomer', async(req,res)=>{
  const monthlyNewCustomers = await customers.aggregate([
    {
      $addFields: {
        created_at_date: { $toDate: "$created_at" }
      }
    },
    {
      $group: {
        _id: { $dateToString: { format: "%Y-%m", date: "$created_at_date" } },
        newCustomers: { $count: {} }
      }
    },
    { $sort: { _id: 1 } }
  ]).toArray();
  res.send(monthlyNewCustomers);
 })

 app.get('/repeatcustomer', async(req,res)=>{
  
 })
 app.get('/location', async(req,res)=>{
  const cityDistribution = await customers.aggregate([
    {
      $group: {
        _id: "$default_address.city",
        customerCount: { $sum: 1 } 
      }
    },
    {
      $sort: { customerCount: -1 } 
    }
  ]).toArray();

  res.send(cityDistribution);
 })
 app.get('/lifetime', async (req, res) => {
  const cltvData = await orders.aggregate([
    // Step 1: Convert 'created_at' to Date and extract the month of the first purchase
    {
      $group: {
        _id: "$customer_id", // Group by customer
        firstPurchase: { $min: { $toDate: "$created_at" } }, // Find the first purchase date for each customer
        totalSpent: { $sum: { $toDouble: "$total_price_set.shop_money.amount" } } // Calculate total spent by each customer
      }
    },
    // Step 2: Group by cohort (month of first purchase)
    {
      $group: {
        _id: { $dateToString: { format: "%Y-%m", date: "$firstPurchase" } }, // Group by month-year of first purchase
        cohortCLTV: { $sum: "$totalSpent" }, // Sum the CLTV for each cohort
        customerCount: { $sum: 1 } // Count number of customers in each cohort
      }
    },
    { $sort: { _id: 1 } } // Sort by cohort
  ]).toArray();

  res.send(cltvData);
});

    // Send a ping to confirm a successful connection
    await client.db("admin").command({ ping: 1 });
    console.log("Pinged your deployment. You successfully connected to MongoDB!");
  } finally {
    // Ensures that the client will close when you finish/error
    // await client.close();
  }
}
run().catch(console.dir);

app.get('/', (req, res) => {
  res.send('Hello World!');
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
});
