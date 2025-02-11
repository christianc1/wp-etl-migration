# 10up ETL Framework

A powerful and flexible ETL (Extract, Transform, Load) framework for WordPress, built by 10up. This framework allows you to easily create data migration pipelines with support for various data sources and destinations.

## Features

- **Flexible ETL Pipeline**: Extract, transform, and load data with configurable pipelines
- **Multiple Data Sources**: Support for various data sources including:
  - CSV files
  - JSON files
  - XML/RSS feeds
  - WordPress (posts, terms, users)
- **WordPress Integration**: Native support for WordPress data types:
  - Posts and Custom Post Types
  - Terms and Taxonomies
  - Users
  - Media attachments
- **Data Transformation**: Robust transformation capabilities with customizable pipelines
- **Ledger System**: Built-in logging and tracking of all data operations
- **Configuration Based**: YAML-based configuration for easy pipeline setup
- **Dependency Management**: Handles complex migration dependencies automatically

## Installation

```bash
composer require christianc1/wp-etl-migration
```

## Example Usage

1. Create a new configuration file.

`migration.yaml`

```yaml
version: 1
name: Example Migration
slug: example_migration
description: Extract data from WordPress, transform it, and reload it into WordPress.
ledger:
  path: 'output/ledgers'

sources:
  localfs:
    type: filesystem
    path: ./imports  # This will be the default path for local files
  wordpress:
    type: wordpress

# Optionally, add a secrets file to store sensitive information. Remember to add this file to your .gitignore!
# secrets: !include "config/secrets.yaml"

migration:
  - !include "config/wp_posts.yaml"
```

`config/wp_posts.yaml`

```yaml
name: wp_posts
skip: false
description: Extract, Transform, and Load WordPress posts into a JSON file.
ledger:
  path: wp_posts
extract:
  - name: wp_posts_extract
    prefix: 'legacy.' # Prefix all fields with 'legacy.'.
    adapter: wp_posts
    args:
      query: # WordPress query arguments
        post_type: post
        post_status: publish
        posts_per_page: 10
        orderby: date
        order: DESC
      withExpandedAuthorData: true # Embed author data for each post
      withTaxonomies: # Embed taxonomical term data for each post
        - category
transform:
  - name: wp_posts_transform
    description: Example transformation pipeline.
    pipeline: PluginNamespace\TransformationPipelines\WPPostsPipeline
load:
  - name: wp_posts_to_json
    loader: JSON
    prefix: wp.
    destination:
      path: ./output/wp_posts
      file: wp-posts.json
    options:
      flags:
        - JSON_PRETTY_PRINT
        - JSON_INVALID_UTF8_SUBSTITUTE
```

2. Register the etl command with WordPress.

```php
// Register the migration command with an optional command prefix
TenupETL\CommandRegistrar\CommandRegistrar::register( $prefix = '' );
```

3. Optionally, register your own custom extractors, transformers, and loaders.

`includes/TransformationPipelines/WPPostsPipeline.php`

```php
/**
 * Post Transformations
 *
 * Handles transformation of legacy post content into WordPress posts during migration.
 *
 * @package ExamplePlugin\TransformationPipelines
 */

namespace ExamplePlugin\TransformationPipelines;

use TenupETL\Classes\Transform\TransformationPipelines\WordPressPostPipeline;

use function TenupETL\Classes\Transform\Transformers\{simple_transformer};
use function Flow\ETL\DSL\{ref, cast, equal, when, lit, entry };

/**
 * Pipeline for transforming post data during migration.
 *
 * @inheritDoc
 */
final class WPPostsPipeline extends WordPressPostPipeline {

    /**
     * The WordPress post type
     *
     * @var string
     */
    public $post_type = 'post';

    /**
     * {@inheritDoc}
     */
    public function prepare(): PostPipeline {
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    protected function post_title(): PostPipeline {
        $this->state
            ->withEntry( $this->core( 'post_title' ), ref( 'legacy.post_title' ) );

        return $this;
    }
}
```

4. Run the migration.

```bash
wp etl process
```

## Configuration

### Global Configuration

The framework uses YAML configuration files to define:

- Data sources and destinations
- Migration steps and dependencies
- Transformation pipelines
- Ledger locations

### Job Configuration

Each migration job can define:

- Extract configurations (source data)
- Transform configurations (data manipulation)
- Load configurations (destination)
- Dependencies on other migrations

## Features in Detail

### Extractors

- **LocalFileSystemExtractor**: Read files from local filesystem
- **CSV/JSON/XML Adapters**: Parse structured data files
- **WordPress Extractors**: Extract data from WordPress

### Transformers

Create custom transformation pipelines by extending the base transformation class:

- Data cleaning
- Field mapping
- Data restructuring
- Custom business logic

### Loaders

- **WordPress Loaders**:
  - `WordPressPostLoader`: Load posts and custom post types
  - `WordPressTermLoader`: Load taxonomies and terms
  - TODO: `WordPressMediaLoader`: Handle media attachments
  - TODO:`WordPressPostTermLoader`: Associate terms with posts
- **JSON Loader**: Export data to JSON files
- **Ledger Loader**: Track migration operations

### Ledger System

The framework maintains detailed ledgers of all operations:

- Track successful migrations
- Record relationships between source and destination data
- Enable incremental updates
- Facilitate rollbacks and auditing

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Credits

Built by Christian Chung @ [10up](https://10up.com).
