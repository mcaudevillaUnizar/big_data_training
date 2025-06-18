# main_script.R

install_and_load <- function(packages) {
  for (pkg in packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }
}

packages <- c("data.table", "purrr", "furrr", "sf", "parallel", "arrow")

install_and_load(packages)

