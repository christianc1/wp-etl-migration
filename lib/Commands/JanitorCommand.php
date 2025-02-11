<?php
/**
 * Class Janitor
 *
 * This class extends the WP_CLI_Command and provides common functionality for all commands.
 * It includes a logger for logging messages and a method to stop ongoing processes or clean up resources.
 *
 * @package TenupETL\Commands
 */

namespace TenupETL\Commands;

use WP_CLI_Command;
use WP_CLI;
use TenupETL\Classes\Config\GlobalConfig;
use TenupETL\Utils\WithLogging;

/**
 * Manages cleanup operations for ETL processes.
 *
 * ## EXAMPLES
 *
 *     # Clean up temporary files
 *     $ wp etl janitor cleanup
 *
 *     # Remove all migration data
 *     $ wp etl janitor reset
 */
class JanitorCommand extends BaseCommand {

	/**
	 * Cleans up temporary files and data from migrations
	 *
	 * ## OPTIONS
	 *
	 * [--older-than=<days>]
	 * : Only clean files older than specified days
	 *
	 * ## EXAMPLES
	 *
	 *     wp etl janitor cleanup
	 *     wp etl janitor cleanup --older-than=7
	 *
	 * @param array $args       Command arguments.
	 * @param array $assoc_args Command associative arguments.
	 */
	public function cleanup( $args, $assoc_args ) {
		// ... implementation ...
	}

	/**
	 * Resets all migration data and removes temporary files
	 *
	 * ## OPTIONS
	 *
	 * [--yes]
	 * : Skip confirmation prompt
	 *
	 * ## EXAMPLES
	 *
	 *     wp etl janitor reset
	 *     wp etl janitor reset --yes
	 *
	 * @param array $args       Command arguments.
	 * @param array $assoc_args Command associative arguments.
	 */
	public function reset( $args, $assoc_args ) {
		// ... implementation ...
	}

	/**
	 * Empties a site of its content.
	 *
	 * @subcommand empty-site
	 */
	public function empty_site() {
		$options = array(
			'return'     => true,
			'launch'     => false,
			'exit_error' => true,
		);

		WP_CLI::runcommand( 'site empty', $options );
		WP_CLI::success( 'Site emptied. Please remember to flush the cache.' );
	}

	/**
	 * Deletes all posts of one or more given post types.
	 *
	 * ## OPTIONS
	 *
	 * [--post_type=<post_types>]
	 * : List of post types to delete separated by comma.
	 *
	 * [--posts_per_page=<posts_per_page>]
	 * : Number of items per page. Defaults to 1000.
	 *
	 * [--yes]
	 * : Proceed to delete the posts without a confirmation prompt.
	 *
	 * @param array $args       Command arguments.
	 * @param array $assoc_args Command associative arguments.
	 *
	 * @subcommand empty-posts
	 */
	public function empty_posts( $args, $assoc_args ) {
		$_post_type      = (string) WP_CLI\Utils\get_flag_value( $assoc_args, 'post_type', '' );
		$_posts_per_page = (int) WP_CLI\Utils\get_flag_value( $assoc_args, 'posts_per_page', 1000 );
		$_posts_per_page = $_posts_per_page > 1000 ? 1000 : $_posts_per_page;

		if ( empty( $_post_type ) ) {
			WP_CLI::error( 'Please provide one (or more) post types separated by a comma.' );
		}

		$options    = array(
			'return'     => true,
			'parse'      => 'json',
			'launch'     => false,
			'exit_error' => true,
		);
		$_posts_ids = WP_CLI::runcommand(
			sprintf(
				'post list --post_type=%s --posts_per_page=%d --field=ID --format=json',
				$_post_type,
				$_posts_per_page
			),
			$options
		);

		if ( empty( $_posts_ids ) ) {
			WP_CLI::warning( 'No posts found.' );
			return;
		}

		WP_CLI::confirm( sprintf( 'Are you sure you want to delete %d posts?', count( $_posts_ids ) ), $assoc_args );

		$_posts_ids = join( ' ', $_posts_ids );
		self::start_bulk_operation();
		WP_CLI::runcommand( "post delete --force --defer-term-counting {$_posts_ids}", $options );
		self::end_bulk_operation();
	}

	/**
	 * Deletes all the terms of one or more given taxonomies.
	 *
	 * ## OPTIONS
	 *
	 * [--taxonomy=<taxonomies>]
	 * : List of taxonomies to delete separated by comma.
	 *
	 * [--yes]
	 * : Proceed to delete the terms without a confirmation prompt.
	 *
	 * @param array $args       Command arguments.
	 * @param array $assoc_args Command associative arguments.
	 *
	 * @subcommand empty-terms
	 */
	public function empty_terms( $args, $assoc_args ) {
		$taxonomy = (string) WP_CLI\Utils\get_flag_value( $assoc_args, 'taxonomy', '' );

		if ( empty( $taxonomy ) ) {
			WP_CLI::error( 'Please provide one (or more) taxonomies separated by a comma.' );
		}

		$options    = array(
			'return'     => true,
			'parse'      => 'json',
			'launch'     => false,
			'exit_error' => true,
		);
		$_terms_ids = WP_CLI::runcommand(
			sprintf(
				'term list %s --field=term_id --format=json',
				$taxonomy
			),
			$options
		);

		if ( empty( $_terms_ids ) ) {
			WP_CLI::error( 'No terms found.' );
		}

		WP_CLI::confirm( sprintf( 'Are you sure you want to delete %d terms?', count( $_terms_ids ) ), $assoc_args );

		$_terms_ids = join( ' ', $_terms_ids );
		self::start_bulk_operation();
		WP_CLI::runcommand( "term delete {$taxonomy} {$_terms_ids}", $options );
		self::end_bulk_operation();
	}

	/**
	 * Cleans the dist directory by running npm clean-dist command.
	 *
	 * @return void
	 */
	public function node_clean_dist() {
		$command    = 'npm run clean-dist';
		$output     = [];
		$return_var = 0;

		exec( $command, $output, $return_var );

		if ( 0 !== $return_var ) {
			WP_CLI::error( 'Failed to clean dist directory' );
		}

		WP_CLI::success( 'Successfully cleaned dist directory' );
	}
}
