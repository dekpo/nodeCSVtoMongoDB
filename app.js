const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');
// URI du serveur MongoDB
const URI = "mongodb+srv://dekpo:qi08xn6@cluster0.dgrcq.mongodb.net/postal_codes?retryWrites=true&w=majority";
// on se connecte au serveur
mongoose.connect(URI, { useNewUrlParser: true, useUnifiedTopology: true}, err => {
    if (err) console.log('ERROR !', err);
    console.log('CONNECTED TO MONGODB !!!');
});
// on définit un schema 
const mySchema = new mongoose.Schema({
    zip: String,
    post_district: String,
    comment: String,
    country_code: String,
    region: String,
    town: String,
    lat: String,
    lng: String
});
// on définit un model
const myModel = mongoose.model('postal_codes',mySchema,'france');
// on peut déclarer une variable i qui nous servira de fusible...
let i = 0;
//fs.createReadStream(path.resolve(__dirname, 'csv_files', 'postal_codes_CH.csv'))
fs.createReadStream(path.resolve(__dirname, 'csv_files', 'postal_codes_FR.csv'))
    .pipe(csv.parse({ headers: true,delimiter:';' }))
    .on('error', error => console.error(error))
    .on('data', row => {
        // mise en place d'un fusible pour stopper le process au moment opportun 
       //if (i==4) process.exit();
        // on sépare les données de latitude et longitude du fichier français
        // du type coordonnees_gps: '47.3909535657,6.4185381191'
        let coords = row.coordonnees_gps.split(',');
        // on peut faire un petit traitement sur le nom de la commune
        // pour n'afficher que la première lettre en majuscule
        let commune = row.Nom_commune.toLowerCase().replace(/\b[a-z]/g, (name) => {
            return name.toUpperCase();
        });
        // idem pour le bureau de poste
        let poste = row.Libelle_d_acheminement.toLowerCase().replace(/\b[a-z]/g, (name) => {
            return name.toUpperCase();
        });
        // les deux premières lettres du code postal sont utilisées pour définir le département
        // pour renseigner la region
        let dept = row.Code_postal.substring(0,2);
        // on définit un nouvel objet bien formaté
       let newRow = {
           zip: row.Code_postal,
           post_district: poste,
           comment: '',
           country_code: 'FR',
           region: dept,
           town: commune,
           lat: coords[0],
           lng: coords[1]
       }

        let postCode = new myModel( newRow );
        postCode.save();

        console.log( newRow );
        i++;
    })    
    .on('end', rowCount => console.log(`Parsed ${rowCount} rows`));