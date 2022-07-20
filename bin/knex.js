var knex =require('knex')({
    client: 'mysql',
    connection: {
        host: '159.65.149.50',
        user: 'root',
        password: 'krowd_voljin_db',
        database: 'sydney_zomato_database'
    },
    pool: {
        min: 0,
        max: 100
    }
});
  
module.exports = knex;
  
