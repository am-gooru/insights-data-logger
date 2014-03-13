Gooru Insights Event Logger
==============
Gooru Insights Event Logger consists of APIs that is required to log event data into Gooru system.


## Dependencies 
<table>
  <tr>
    <th style="text-align:left;">JDK</th>
    <td>1.6 or above</td>
  </tr>
  <tr>
    <th style="text-align:left;">Operating System</th>
    <td>Ubuntu</td>
  </tr>
   <tr>
    <th style="text-align:left;">Application container</th>
    <td>Apache tomcat7</td>
  </tr>
   <tr>
    <th style="text-align:left;">Apache Maven</th>
    <td>Maven 3.0.4</td>
  </tr>
  <tr>
    <th style="text-align:left;">Kafka</th>
    <td>0.7.2</td>
  </tr>
  <tr>
    <th style="text-align:left;">Cassandra</th>
    <td>1.2.6</td>
  </tr>
</table>

## Build
* Update your tomcat location in "webapp.container.home" property in pom.xml
For example, `<webapp.container.home>${env.CATALINA_HOME}</webapp.container.home>`
* Navigate to the development project folder.
For example, cd Home\Projects\Insights-Event-Logger
* From the linux terminal Clean install the build.
Command: `mvn clean install -P dev -Dmaven.test.skip=true`
* Project deployed on `<webapp.container.home>/webapps/` location


## License
Gooru Insights Event Logger is released under the [MIT License](http://opensource.org/licenses/MIT) .
