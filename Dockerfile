FROM rocker/rstudio

# # # # # # # # # # # # # # #
# install system dependencies
# # # # # # # # # # # # # # #
USER root

RUN apt-get update && apt-get install zlib1g-dev libxml2-dev -y

# # # # # # # # # # # # # # #
# install R dependencies
# # # # # # # # # # # # # # #
RUN R -e "install.packages('remotes', repos = 'http://cran.us.r-project.org')"
RUN R -e 'remotes::install_github("rstudio/renv")'
WORKDIR /home/rstudio/
COPY renv.lock ./
RUN R -e "renv::restore(lockfile = 'renv.lock',library = '/usr/local/lib/R/library',actions = c('install', 'upgrade'))"

# Configure users and start R studio
RUN adduser rstudio sudo
RUN sed -i "s/#!\/usr\/bin\/with-contenv bash/#!\/bin\/bash/g" /etc/cont-init.d/userconf
RUN printf "#!/bin/bash \n\
/etc/cont-init.d/userconf \n\
exec /usr/lib/rstudio-server/bin/rserver --server-daemonize=0 --server-app-armor-enabled=0" >> /mnt/run_me.sh

RUN chmod 755 /mnt/run_me.sh
RUN chmod 755 /etc/cont-init.d/userconf

WORKDIR /home/rstudio/
