FROM jruby:9.1.7.0


COPY ./ /jruby_kata
RUN apt-get update && apt-get install -y vim
RUN cd /jruby_kata && gem install jbundler && bundle install && jbundle install

