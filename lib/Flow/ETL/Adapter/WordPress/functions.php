<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\{Attribute\DocumentationDSL, Attribute\DocumentationExample, Attribute\Module, Attribute\Type as DSLType};

/**
 * Creates a WordPress posts loader
 *
 * @param array $config Configuration options for the loader
 */
#[DocumentationDSL(module: Module::WORDPRESS, type: DSLType::LOADER)]
#[DocumentationExample(topic: 'wordpress', example: 'posts')]
function to_wp_posts(array $config = []): WPPostsLoader
{
    return new WPPostsLoader($config);
}

/**
 * Creates a WordPress posts extractor
 *
 * @param array $config Configuration options for the extractor
 */
#[DocumentationDSL(module: Module::WORDPRESS, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'wordpress', example: 'posts')]
function from_wp_posts(array $config = []): WPPostsExtractor
{
    return new WPPostsExtractor($config);
}

/**
 * Creates a WordPress terms extractor
 *
 * @param array $config Configuration options for the extractor
 */
#[DocumentationDSL(module: Module::WORDPRESS, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'wordpress', example: 'terms')]
function from_wp_terms(array $config = []): WPTermExtractor
{
    return new WPTermExtractor($config);
}

/**
 * Creates a WordPress users extractor
 *
 * @param array $config Configuration options for the extractor
 */
#[DocumentationDSL(module: Module::WORDPRESS, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'wordpress', example: 'users')]
function from_wp_users(array $config = []): WPUserExtractor
{
    return new WPUserExtractor($config);
}
