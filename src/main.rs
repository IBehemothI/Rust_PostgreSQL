/* Descripción:
Sebastián Paredes
Codificación: 14-01-2021 <-> 17-01-2021
Pruebas: 17-01-2021 <-> 18-01-2021
Proceso de migración de datos CSV a base de datos PostgreSQL con Rust.*/

//Subir CSV
use std::io::Write;
use actix_multipart::Multipart;
//Framework Rust(Actix)
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use futures::{StreamExt, TryStreamExt};
//Conexión a postgreSQL
extern crate postgres;
use postgres::{Connection, TlsMode};
//Chequear posiciones pares e impares
extern crate is_even;
use is_even::IsEven;
//Guardar elementos sin tildes o caracteres
use unicode_normalization::UnicodeNormalization;
//Manejo de errores
use std::error::Error as Error1;
//Lector de archivo
use csv::ReaderBuilder;
//Uso de path para el archivo
use std::path::PathBuf;
//Uso de Datetimes
extern crate chrono;
use chrono::{NaiveDate, Duration, Utc};

async fn guardar_archivo(mut payload: Multipart) -> Result<HttpResponse, Error> {
    let mut auxnombre = String::from(""); //Variable auxiliar para tomar el nombre del archivo.
    while let Ok(Some(mut field)) = payload.try_next().await {
        let tipo = field.content_disposition().unwrap();
        let archivo_nombre = tipo.get_filename().unwrap();
        auxnombre.push_str(archivo_nombre);
        let archivo_path = format!("./{}", sanitize_filename::sanitize(&archivo_nombre));
        //println!("{:?}", archivo_path);
        let mut f = web::block(|| std::fs::File::create(archivo_path))
            .await
            .unwrap();
        while let Some(chunk) = field.next().await {
            let datos = chunk.unwrap();
            f = web::block(move || f.write_all(&datos).map(|_| f)).await?;
        }
    }
    "ok";
    migracion(auxnombre);  
    //Página de cargado. 
    let html = r#"<html> 
        <head><title>Migraci&oacute;n Completa</title></head>
        <body>
        <font color="darkblue" size="6">
            <b>P&aacute;gina de confirmaci&oacute;n</b><br><br>
            <font color="blue" size="4">
            <b>Los datos han sido migrados correctamente!</b>
            <br><br>            
        </body>
    </html>"#; 
    Ok(HttpResponse::Ok().body(html))
}

fn index() -> HttpResponse {
    let html = r#"<html>
        <head><title>Carga de CSV</title></head>
        <body>
        <font color="darkblue" size="6">
            <b>P&aacute;gina de carga de CSV</b><br><br>
            <font color="blue" size="4">
            <b>Puede subir el archivo CSV desde esta p&aacute;gina:</b>
            <br><br>
            <font color="black" size="3">
            <form target="/" method="post" enctype="multipart/form-data">
                <input type="file" multiple name="file"/>
                <br><br>
                <button type="submit">&nbsp;Subir Archivo</button>
            </form>
        </body>
    </html>"#;
    HttpResponse::Ok().body(html)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let url = "postgresql://postgres:123@localhost:5432/base";
    let _conexion = Connection::connect(url, TlsMode::None).unwrap();
    //////////////////////////////////////////
    _conexion.batch_execute("
    CREATE TABLE PERSONA(
        ID VARCHAR(20),
        NOMBRE VARCHAR(50),
        GENERO VARCHAR(1),
        ESTADO VARCHAR(25),
        FECHA DATE,
        TELEFONO VARCHAR(20),
        DIRECCION VARCHAR(50),
        EMAIL VARCHAR(50),
        VALIDADO INT4,
        OBSERVACION VARCHAR(500)
    );").unwrap();
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    std::fs::create_dir_all("./").unwrap();
    let ip = "0.0.0.0:3000";
    HttpServer::new(|| {
        App::new().wrap(middleware::Logger::default()).service(
            web::resource("/")
                .route(web::get().to(index))
                .route(web::post().to(guardar_archivo)),
        )
    })
    .bind(ip)?
    .run()
    .await    
}

fn migracion(nombre:String)
{    
    //_conexion.finish();
    let nombreaux = String::from(nombre);
    let mut path = PathBuf::new();
    path.push("./");
    path.push(nombreaux);
    let nombreaux=path.into_os_string().into_string().unwrap();
    println!("{:?}", nombreaux);
    if let Err(e) = leer_archivo(&nombreaux) {
        eprintln!("{}", e);
    }
}


fn escedula (field:&str) -> bool
{
    let mut valido:i32 = 1;
    let stringaux1= field.chars().nth(0).unwrap().to_string();
    let primero : i32 = stringaux1.parse().unwrap();
    let stringaux2= field.chars().nth(1).unwrap().to_string();
    let segundo : i32 = stringaux2.parse().unwrap();
    let stringaux3= field.chars().nth(2).unwrap().to_string();
    let tercero : i32 = stringaux3.parse().unwrap();
    if primero >= 0 && primero < 2 && segundo <=9 && segundo >=0 && tercero < 6
    {
        valido=1;
    }
    else if primero == 2 && segundo >=0 && segundo <=4 && tercero < 6
    {
        valido=1;
    }
    else if primero == 3 && segundo ==0 && tercero < 6
    {
        valido=1;
    }
    else if primero == 5 && segundo ==0 && tercero < 6
    {
        valido=1;
    }
    else if primero == 8 && segundo ==0 && tercero < 6
    {
        valido=1;
    }
    else
    {
        valido=0;
    }
    let mut suma:i32 = 0;
    if valido==1
    {
        for i in 0..9
        {
            let aux = i as i32;
            let stringaux1= field.chars().nth(i).unwrap().to_string();
            let aux2 : i32 = stringaux1.parse().unwrap();
            if aux.is_even() == true || aux == 0
            {                                
                if (aux2 * 2) >= 10
                {
                    suma += (aux2 * 2) - 9;
                }                                
                else
                {
                    suma += aux2 * 2;
                }
            }
            else if aux.is_even() == false
            {
                suma += aux2;
            }
        }
    }
    else{}
    //println!("{:?}SUMA", suma);
    if suma < 10 && valido==1
    {
        let stringaux1= field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 10 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido=1;
        }
        else
        {
            valido=0;
        }
    }
    else if suma >= 10 && suma < 20 && valido==1
    {
        let stringaux1= field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 20 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido=1;
        }
        else
        {    
            valido=0;
        }
    }
    else if suma >= 20 && suma < 30 && valido==1
    {
        let stringaux1= field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 30 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido=1;
        }
        else
        {    
            valido=0;
        }
    }
    else if suma >= 30 && suma < 40 && valido==1
    {
        let stringaux1= field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 40 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido=1;
        }
        else
        {    
            valido=0;
        }
    }
    else if suma >= 40 && suma < 50 && valido==1
    {
        let stringaux1= field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 50 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido=1;
        }
        else
        {    
            valido=0;
        }
    }
    else if suma >= 50 && suma < 60 && valido==1
    {
        let stringaux1= field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 60 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido=1;
        }
        else
        {    
            valido=0;
        }
    }
    else if suma >= 60 && suma < 70 && valido==1
    {
        let stringaux1= field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 70 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido=1;
        }
        else
        {    
            valido=0;
        }
    }
    else if suma >= 70 && suma < 80 && valido==1
    {
        let stringaux1 = field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 80 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido=1;
        }
        else
        {   
            valido=0;
        }
    }
    else if suma >= 80 && suma < 90 && valido==1
    {
        let stringaux1= field.chars().nth(9).unwrap().to_string();
        let aux2 : i32 = stringaux1.parse().unwrap();
        let mut digitov = 90 - suma;
        if digitov==10
        {
            digitov=0;
        }
        if aux2 == digitov
        {
            valido= 1;
        }
        else
        {   
            valido=0;
        }
    }
    else{}
    //println!("{:?}Valido", valido);
    if field=="9999999999"
    {
        valido=1;
    }
    else{}

    if valido==1
    {
        return true;
    }
    else{return false;}
}

fn esnumerico(field:&str) -> bool{
    for i in 0..10 {
        let stringaux1= field.chars().nth(i).unwrap().to_string();
        let mut aux2 = stringaux1.chars();
        if aux2.any(|c| matches!(c, '0'..='9'))
        {
        }        
        else
        {
            return false;
        }
    }
    return true;
}

fn convertirfecha(field:&str) -> bool{
    let test = NaiveDate::parse_from_str(&field.to_string(), "%Y-%m-%d");
     match test {
        Ok(ok) => return true,
        Err(e) => return false, 
    }
}

/*fn fecha_antes(fecha: NaiveDate<>) -> Option<NaiveDate<>> {
    fecha.checked_sub_signed(Duration::weeks(4))
}*/

fn leer_archivo(path: &str) -> Result<() , Box<dyn Error1>> {
    let mut reader = ReaderBuilder::new().has_headers(false).delimiter(b';').from_path(path)?;
    let url = "postgresql://postgres:123@localhost:5432/base";
    let _conexion = Connection::connect(url, TlsMode::None).unwrap();
    for result in reader.records() {
        let record = result?;
        let mut complemento=String::from("(");
        let mut sentencia = String::from("INSERT INTO PERSONA(ID,NOMBRE,GENERO,ESTADO,FECHA,TELEFONO,DIRECCION,EMAIL,VALIDADO,OBSERVACION)
    VALUES ");
        let mut contador=0;
        let mut valido: u32 = 1;
        let mut observacion="".to_string(); 
        for field in &record {        
            contador=contador+1;
            //observacion="".to_string();  
            if field!="" && contador==1 //NÚMERO DE DOCUMENTO DE IDENTIFICACIÓN
            {
                if field.chars().count()==10 && field.chars().all(char::is_numeric)
                {//Cédula
                    if escedula(field)==false
                    {
                        observacion.push_str("Error en la cédula.");
                        //valido=0;
                    }
                    else if escedula(field)==true
                    {
                        //valido=1;
                    }
                }
                
                else if field.chars().count()>=5 && field.chars().count()<=20 && field.chars().all(char::is_alphanumeric)
                {//Pasaporte
                    if field.chars().any(|c| matches!(c, 'a'..='z')) || field.chars().any(|c| matches!(c, 'A'..='Z')) || field.chars().any(|c| matches!(c, '0'..='9')) || field.contains('ñ') || field.contains('Ñ')
                    {
                        //valido = 1;
                        observacion.push_str("Identificación puede ser pasaporte.");
                    }
                    if field.chars().count()>=10 //VALIDACIÓN POSIBLE CÉDULA
                    {
                        if esnumerico(field)==true
                        {
                            if escedula(field)==true
                            {
                                valido=1;
                                observacion.push_str("Es cédula, no pasaporte.");
                            }
                            else if escedula(field)==false
                            {
                            }
                        }
                        else{}
                    }   
                    else
                    {
                        valido = 0;
                        observacion.push_str("Error en el pasaporte.");
                    }
                }
                else if field.chars().count()==13 && field.chars().all(char::is_numeric)
                {//RUC     
                    valido=1;
                    observacion.push_str("Identificación es RUC.");
                }
                else
                {
                    //valido=0;
                    observacion.push_str("Error en el Número de Identificación.");
                }
                //println!("{:?}OBS2", observacion);
                complemento.push_str("'");
                complemento.push_str(field);
                complemento.push_str("',");
                //println!("{:?}VALIDO1", valido);
            } 
            else if field!="" && contador==2 //NOMBRE DE LA PERSONA
            {
                if field.chars().any(|c| matches!(c, '0'..='9'))
                {
                    let nombre = &field.to_string();                   
                    let nombre_ascii: String = nombre.nfd().filter(char::is_ascii).collect();
                    let mayus=nombre_ascii.to_uppercase();
                    complemento.push_str("'");
                    complemento.push_str(&mayus);
                    complemento.push_str("',");
                    valido=0;
                    observacion.push_str(" Error en el nombre, contiene números.");
                }
                else if field.contains(' ')
                {
                    let nombre = &field.to_string();                   
                    let nombre_ascii: String = nombre.nfd().filter(char::is_ascii).collect();
                    let mayus=nombre_ascii.to_uppercase();
                    if field.contains(',') || field.contains('/') || field.contains('-') || field.contains(';') || field.contains('.') || field.contains('_') || field.contains('(') || field.contains(')') || field.contains('%') || field.contains('#') || field.contains('"') || field.contains('!') || field.contains('¿') || field.contains('|') || field.contains('°') || field.contains('\'') || field.contains('\\') 
                    {
                        complemento.push_str("");
                        complemento.push_str("null");
                        complemento.push_str(",");
                        valido=0;
                        observacion.push_str(" Error en el nombre, contiene caracteres especiales.");
                    }
                    else if field.chars().any(|c| matches!(c, 'a'..='z')) || field.chars().any(|c| matches!(c, 'A'..='Z')) 
                    {
                        complemento.push_str("'");
                        complemento.push_str(&mayus);
                        complemento.push_str("',");
                        valido=1;
                    }
                    else{}                    
                }
                else if field.contains(',') || field.contains('/') || field.contains('-') || field.contains(';') || field.contains('.') || field.contains('_') || field.contains('(') || field.contains(')') || field.contains('%') || field.contains('#') || field.contains('"') || field.contains('!') || field.contains('¿') || field.contains('|') || field.contains('°') || field.contains('\'') || field.contains('\\') 
                {
                    complemento.push_str("");
                    complemento.push_str("null");
                    complemento.push_str(",");
                    valido=0;
                    observacion.push_str(" Error en el nombre, contiene caracteres especiales.");
                }
                else
                {
                    let nombre = &field.to_string();                   
                    let nombre_ascii: String = nombre.nfd().filter(char::is_ascii).collect();
                    let mayus=nombre_ascii.to_uppercase();
                    complemento.push_str("'");
                    complemento.push_str(&mayus);
                    complemento.push_str("',");
                    valido=0;
                    observacion.push_str(" Error en el nombre.");
                }
            }     
            else if field!="" && contador==3 //GÉNERO DE LA PERSONA
            {
                let genero = &field.to_string();                   
                let mayus=genero.to_uppercase();
                if mayus=="M" || mayus=="F" 
                {
                    complemento.push_str("'");
                    complemento.push_str(&mayus);
                    complemento.push_str("',");
                    //valido=1;
                }
                else if mayus=="NULL"
                {
                    let minus=mayus.to_lowercase();
                    complemento.push_str("");
                    complemento.push_str(&minus);
                    complemento.push_str(",");
                    //valido=1;
                }
                else if field.chars().count()>1
                {
                    complemento.push_str("");
                    complemento.push_str("null");
                    complemento.push_str(",");
                    //valido=0;
                    observacion.push_str(" Error en el género.");
                }
                else if field=="" ||field==" "
                {
                    complemento.push_str("null");
                    complemento.push_str(",");
                }
                else
                {
                    complemento.push_str("'");
                    complemento.push_str(&mayus);
                    complemento.push_str("',");
                    //valido=0;
                    observacion.push_str(" Error en el género.");
                }
            }  
            else if field!="" && contador==4 //ESTADO CIVIL DE LA PERSONA
            {
                let estado = &field.to_string();                   
                let mayus=estado.to_uppercase();
                if mayus=="SOLTERO" || mayus=="CASADO" || mayus=="DIVORCIADO" || mayus=="VIUDO" || mayus=="EN UNION DE HECHO"
                {
                    complemento.push_str("'");
                    complemento.push_str(&mayus);
                    complemento.push_str("',");
                    //valido=1;
                }
                else if mayus=="NULL"
                {
                    let minus=mayus.to_lowercase();
                    complemento.push_str("");
                    complemento.push_str(&minus);
                    complemento.push_str(",");
                    //valido=1;
                }
                else if field=="" ||field==" "
                {
                    complemento.push_str("null");
                    complemento.push_str(",");
                }
                else
                {
                    complemento.push_str("'");
                    complemento.push_str(&mayus);
                    complemento.push_str("',");
                    //valido=0;
                    observacion.push_str(" Error en el estado civil.");
                }
            } 
            else if field!="" && contador==5 //FECHA DE NACIMIENTO DE LA PERSONA
            {
                let fechahoy= Utc::today().format("%Y-%m-%d");
                if convertirfecha(field)==true
                {
                    let fechaaux1 = NaiveDate::parse_from_str(&field.to_string(), "%Y-%m-%d").unwrap();
                    let fechaaux2 =  NaiveDate::parse_from_str(&fechahoy.to_string(), "%Y-%m-%d").unwrap();
                    let b = fechaaux2 - fechaaux1;
                    if b >= Duration::seconds(252300000) && b <= Duration::seconds(2996000000)
                    {
                        complemento.push_str("'");
                        complemento.push_str(field);
                        complemento.push_str("',");
                    }
                    else
                    {
                        complemento.push_str("'");
                        complemento.push_str(field);
                        complemento.push_str("',");
                        observacion.push_str(" La fecha no está en el rango.");
                    }
                }
                else if convertirfecha(field)==false
                {
                    complemento.push_str("null");
                    complemento.push_str(",");
                    observacion.push_str(" La fecha no es válida.");
                }
            } 
            else if field!="" && contador==6 //TELEFONO DE LA PERSONA
            {
                println!("{:?}N", field);
                if field.chars().count()<=9 && field.chars().all(char::is_numeric)
                {//Convencional
                    complemento.push_str("'593");
                    if field.chars().count()==9
                    {
                        let stringaux1= field.chars().nth(0).unwrap().to_string();
                        let primero : i32 = stringaux1.parse().unwrap();
                        let stringaux2= field.chars().nth(1).unwrap().to_string();
                        let segundo : i32 = stringaux2.parse().unwrap();                        
                        if primero == 0 && segundo >= 2 && segundo <=7
                        {//Ecuador
                            //complemento.push_str("'593");
                            complemento.push_str(field);
                            complemento.push_str("',");
                            if valido==1
                            {
                                valido=1;
                            }
                            else if valido==0
                            {
                                valido=0;
                            }
                            else{}
                        }
                        else
                        {
                            complemento.push_str(field);
                            complemento.push_str("',");
                            valido=0;
                            observacion.push_str(" Códigos de provincia incorrectos.");
                        }
                    }
                    else if field.chars().count()>=6
                    {//Extranjero
                        complemento.push_str(field);
                        complemento.push_str("',");
                        observacion.push_str(" Teléfono no es válido.");
                        valido=0;
                    }
                    
                }
                else if field.chars().count()==10 && field.chars().all(char::is_numeric)
                {//Celular
                    let stringaux1= field.chars().nth(0).unwrap().to_string();
                    let primero : i32 = stringaux1.parse().unwrap();
                    let stringaux2= field.chars().nth(1).unwrap().to_string();
                    let segundo : i32 = stringaux2.parse().unwrap();
                    if primero == 0 && segundo == 9
                    {       
                        complemento.push_str("'");
                        complemento.push_str(field);
                        complemento.push_str("',");
                        valido=1;
                        observacion.push_str(" El número es un celular.");
                    }
                    else
                    {
                        complemento.push_str("'");
                        complemento.push_str(field);
                        complemento.push_str("',");
                        valido=0;
                        observacion.push_str(" El número de celular no es de Ecuador.");
                    }
                }                
                else
                {
                    complemento.push_str("'");
                    complemento.push_str(field);
                    complemento.push_str("',");
                    valido=0;
                    observacion.push_str(" Error en el número de teléfono.");
                }

            } 
            else if field!="" && contador==7 //DIRECCION DE LA PERSONA
            {
                if field.contains(' ')
                {
                    complemento.push_str("'");
                    complemento.push_str(field);
                    complemento.push_str("',");
                }
                else if !field.contains(' ')
                {
                    complemento.push_str("'");
                    complemento.push_str(field);
                    complemento.push_str("',");
                    observacion.push_str(" Dirección no es válida.");
                }
            } 
            else if field!="" && contador==8 //EMAIL DE LA PERSONA
            {//Toma válido que sea .com
                let posaux= field.find("@").ok_or("no @")?;
                //println!("{:?}@", posaux);
                if field.contains(' ')
                {
                    valido=0;
                    complemento.push_str("'");
                    complemento.push_str(field);
                    complemento.push_str("',");
                    observacion.push_str(" Email no debe contener espacios.");
                }
                else if !field.contains(' ')
                {
                    let pos_aux= field.chars().nth(posaux-1).unwrap().to_string();
                    let pos_aux1= field.chars().nth(posaux+1).unwrap().to_string();
                    if pos_aux=="."
                    {
                        valido=0;
                        complemento.push_str("'");
                        complemento.push_str(field);
                        complemento.push_str("',");
                        observacion.push_str(" Email no debe contener . antes del @.");
                    }
                    else if pos_aux1=="."
                    {
                        valido=0;
                        complemento.push_str("'");
                        complemento.push_str(field);
                        complemento.push_str("',");
                        observacion.push_str(" Email no debe contener . después del @.");
                    }
                    else
                    {
                        let string_pos = field.find("@").ok_or("no @").unwrap().to_string();
                        let mut posarroba = string_pos.parse::<i32>().unwrap();
                        posarroba = posarroba + 1;
                        //println!("{:?}@", posaux);
                        //println!("{:?}posar", posarroba);
                        let string_pos1= field.chars().count().to_string();
                        let mut posfin = string_pos1.parse::<i32>().unwrap();
                        posfin = posfin - 1;
                        //println!("{:?}fin1", field.chars().count());
                        //println!("{:?}fin2", posfin);
                        let resta : i32 = posfin - posarroba;
                        if resta >=6 && resta <=10
                        {
                            if valido==1
                            {
                                valido=1;
                            }
                            else if valido==0
                            {
                                valido=0;
                            }
                            else{}
                            complemento.push_str("'");
                            complemento.push_str(field);
                            complemento.push_str("',");
                        }
                        else
                        {
                            valido=0;
                            complemento.push_str("'");
                            complemento.push_str(field);
                            complemento.push_str("',");
                            observacion.push_str(" Email no tiene un dominio válido.");
                        }                                                
                    }
                }
            } 
            else
            {
                complemento.push_str("null");
                complemento.push_str(",");
            }  
            //println!("{:?}OBS3", observacion);                   
        } 
        complemento.push_str("'");
        complemento.push_str(&valido.to_string());
        complemento.push_str("',");
        complemento.push_str("'");
        complemento.push_str(&observacion);
        complemento.push_str("')");
        //println!("{}",complemento);
        sentencia.push_str(&complemento);
        println!("{}",sentencia);
        _conexion.batch_execute(&sentencia).unwrap();        
    }
    Ok(())
}
