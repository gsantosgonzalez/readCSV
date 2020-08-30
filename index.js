const redis = require('redis');
const fs = require('fs');
const { argv } = require('process');

if (!argv[2] || !argv[3]) {
  console.log('Se requieren dos parámetros:\n\tEl nombre del archivo a leer\n\tEl nombre del archivo de salida')
  process.exit(1)
}

if (!argv[2].trim().endsWith('csv') && !argv[2].trim().endsWith('json')) {
  console.log('Solamente se permite leer de archivos csv o json', argv[2], 'Fail')
  process.exit(2)
}

let client = null;
let campAuxes = []
let isCSV = false;

(async () => {
  const connectRedis = () => {
    // Esta es la BD de redis del productivo.
    client = redis.createClient('3004', '172.20.20.104');

    client.on('connect', function() {
      console.log('connected');
    });

    client.select(2);
  }
  await connectRedis();

  if (argv[2].endsWith('csv')) {
    isCSV = true
    // Archivo con la lista de campos auxiliares de mongo que no tienen otros campos.
    const contents = fs.readFileSync(argv[2]);

    // obtenemos un array con todos los campos auxiliares
    campAuxes = contents.toString().split('\n');

    // eliminamos el encabezado si existe
    if (campAuxes[0].includes('camp_aux'))
      campAuxes.shift()

    // eliminamos la última línea si está vacía
    if (campAuxes[campAuxes.length - 1] === '')
      campAuxes.pop()
  } else {
    //Obtenemos los campos auxiliares del archivo json.
    campAuxes = require(`./${argv[2]}`)
  }


  // El archivo en que se va a escribir.
  const file = `${__dirname}/${argv[3]}`

  if (!fs.existsSync(file)) {
    // Se inserta el corchete que abre el archivo.
    fs.writeFileSync(file, '[');
  }
console.log(campAuxes.length, 'Cuántos son?')
  for (let i = 0; i < campAuxes.length; i++) {
    let camp = !isCSV ? campAuxes[i].camp_aux : campAuxes[i]

    if (isCSV) {
      // Eliminamos las comillas y el caracter de retorno de carro
      camp = camp.endsWith('\r') ? camp.slice(1, -2) : camp.slice(1, -1)
    }

    client.hgetall(camp, (error, response) => {
      if (error) console.log('Error')

      if (response) {
        let text = `{"message":"${response.message}","number":"${response.number}","pnn":{"operator":"${response.operator || 'Sin Operador'}","city":"${response.city || 'Sin Ciudad'}","town":"${response.town || 'Sin Localidad'}","region":"${response.region || 'Sin Region'}"}, "saved_date":"${response.date}", "user":"${response.user}", "uuid":"${response.uuid}", "camp_aux":"${camp}"}`

        if (i < campAuxes.length - 1) {
          text = text + ','
        } else {
          text = text + ']'
        }

        fs.appendFileSync(file, text);
      }

      if (i >= campAuxes.length -1) {
        process.exit(0)
      }
    })
  }

  // OJO: Al finalizar la creación del archivo es preciso agregar el corchete de cierre del arreglo.
})();
