# main_script.R
install_and_load <- function(packages) {
  for (pkg in packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }
}

# Paquetes que usaremos
packages <- c("data.table", "purrr", "furrr", "sf", "parallel", "arrow", "mapSpain", "dplyr", "future")
install_and_load(packages)

# Obtener polígono de Zaragoza
zaragoza <- esp_get_prov(prov = "Zaragoza", moveCAN = FALSE)
wkt_zaragoza <- st_as_text(st_geometry(zaragoza))

# Preparar paralelización
library(future)
num_cores <- max(parallel::detectCores() - 1, 2)  # al menos 2 cores
plan(multisession, workers = num_cores)

# Listar archivos gpkg descargados en raw_data
files <- list.files("raw_data", pattern = "\\.gpkg$", full.names = TRUE)

# Función para procesar cada archivo
process_file <- function(file_path) {
  sf_data <- sf::read_sf(file_path, wkt_filter = wkt_zaragoza)
  # Seleccionar columnas de interés
  sf_data <- dplyr::select(sf_data, MeanTemperature, Precipitation, RelativeHumidity)
  
  data.table::data.table(
    filename = basename(file_path),
    MeanTemperature = mean(sf_data$MeanTemperature, na.rm = TRUE),
    Precipitation = mean(sf_data$Precipitation, na.rm = TRUE),
    RelativeHumidity = mean(sf_data$RelativeHumidity, na.rm = TRUE)
  )
}

# Procesar todos los archivos en paralelo
results_list <- furrr::future_map(files, process_file)

# Combinar resultados en un data.table
results_dt <- data.table::rbindlist(results_list)

# Añadir columna fecha a partir del nombre del archivo (meteo_YYYYMMDD.gpkg)
results_dt[, date := as.Date(sub("meteo_(\\d{8})\\.gpkg", "\\1", filename), format = "%Y%m%d")]

# Ordenar por fecha
results_dt <- results_dt[order(date)]

# Mostrar resultados
print(results_dt)

# Crear carpeta results si no existe
if (!dir.exists("results")) dir.create("results")

# Guardar resultados en CSV
data.table::fwrite(results_dt, "results/daily_avg_zaragoza_april2025.csv")

# Finalizar plan paralelización para volver a secuencial
plan(sequential)

if (file.exists("data/daily_avg_zaragoza_april2025.csv")) {
  cat("✅ Archivo CSV generado correctamente en 'data/'.\n")
} else {
  cat("❌ No se encontró el archivo en 'data/'.\n")
}

