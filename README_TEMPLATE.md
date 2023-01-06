```js
// {{ TEMPLATE: }}
module.exports = {
  CUSTOM_PINNED_REPOS: {
    type: 'specificRepos',
    repos: [
      'vidl',
      'golang/go',
      'probablykasper/embler',
    ],
    modifyVariables: function(repo, moment, user) {
      repo.REPO_CREATED_MYDATE = moment(repo.REPO_CREATED_TIMESTAMP).format('YYYY MMMM Do')
      return repo
    },
  },
  "2_MOST_STARRED_REPOS": {
    type: 'repos',
    params: `
      first: 2,
      privacy: PUBLIC,
      ownerAffiliations:[OWNER],
      orderBy: { field:STARGAZERS, direction: DESC },
    `,
  },
  LATEST_VIDL_RELEASE: {
    type: 'customQuery',
    loop: false,
    query: async (octokit, moment, user) => {
      // You can do anything  you want with the GitHub API here.
      const result = await octokit.graphql(`
        query {
          repository(name: "spark-clickhouse-plugin", owner: "The-Analytics-Gladiators") {
            releases(last: 1) {
              edges {
                node {
                  url
                  publishedAt
                  tagName
                }
              }
            }
          }
        }
      `)
      const release = result.repository.releases.edges[0].node
      const date = new Date(release.publishedAt)
      // We have `loop: false`, so we return an object.
      // If we had `loop: true`, we would return an array of objects.
      return {
        VIDL_RELEASE_TAG: release.tagName,
        VIDL_RELEASE_URL: release.url,
        VIDL_RELEASE_WHEN: moment(release.publishedAt).fromNow(),
      }
    }
  }
}
// {{ :TEMPLATE }}
```


# Mighty Spark Clickhouse Plugin

[![spark-clickhouse-plugin](https://circleci.com/gh/The-Analytics-Gladiators/spark-clickhouse-plugin.svg?style=svg)](https://circleci.com/gh/The-Analytics-Gladiators/spark-clickhouse-plugin)
![https://circleci.com/gh/The-Analytics-Gladiators/spark-clickhouse-plugin](https://img.shields.io/github/license/The-Analytics-Gladiators/spark-clickhouse-plugin)
![https://circleci.com/gh/The-Analytics-Gladiators/spark-clickhouse-plugin](https://img.shields.io/github/v/tag/The-Analytics-Gladiators/spark-clickhouse-plugin)

The most intuitive Spark Plugin for interacting with Clickhouse

## Usage

### Maven
```xml
  <profiles>
    <profile>
      <id>github</id>
      <repositories>
        <repository>
          <id>central</id>
          <url>https://repo1.maven.org/maven2</url>
        </repository>
        <repository>
          <id>github</id>
          <url>https://maven.pkg.github.com/OWNER/REPOSITORY</url>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
      </repositories>
    </profile>
  </profiles>
  <dependencies>
  <dependency>
        <groupId>io.gladiators</groupId>
        <artifactId>spark-clickhouse-plugin_2.12</artifactId>
        <version>VIDL_RELEASE_TAG</version>
    </dependency> 
  </dependencies>

```


## Contributions

Contributions are welcome, but there are no guarantees that they are accepted as such. Process for contributing is the following:

* Fork this project
* Create an issue to this project about the contribution (bug or feature) if there is no such issue about it already. Try to keep the scope minimal.
* Develop and test the fix or functionality carefully. Only include minimum amount of code needed to fix the issue.
* Refer to the fixed issue in commit
* Send a pull request for the original project
* Comment on the original issue that you have implemented a fix for it


License

Spark Clickhouse Plugin is licensed under the MIT license. See the LICENSE.txt file for more information.