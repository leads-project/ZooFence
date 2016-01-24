FROM java:7
# Install maven
RUN wget http://mirror.easyname.ch/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
RUN tar xf apache-maven-3.3.9-bin.tar.gz
RUN rm apache-maven-3.3.9-bin.tar.gz
RUN mv apache-maven-3.3.9 /usr/share/maven
RUN ln -s /usr/share/maven/bin/mvn /usr/local/bin/mvn

RUN git clone https://github.com/otrack/menagerie.git
WORKDIR menagerie
RUN mvn clean install -DskipTests

WORKDIR /
RUN git clone https://github.com/leads-project/ZooFence.git
WORKDIR /ZooFence
ENTRYPOINT ["mvn", "test", "-Dtest=DockerTest"]


