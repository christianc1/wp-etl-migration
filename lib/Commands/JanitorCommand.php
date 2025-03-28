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
	 * [--batch-size=<batch-size>]
	 * : Number of posts to delete in a single batch. Defaults to 100.
	 *
	 * [--yes]
	 * : Skip confirmation prompt.
	 *
	 * ## EXAMPLES
	 *
	 *     # Delete all posts of type 'post'
	 *     $ wp etl janitor empty-posts --post_type=post
	 *
	 *     # Delete all posts of types 'post' and 'page' without confirmation
	 *     $ wp etl janitor empty-posts --post_type=post,page --yes
	 *
	 *     # Delete posts in larger batches (500 at a time)
	 *     $ wp etl janitor empty-posts --post_type=post --batch-size=500
	 *
	 * @param array $args       Command arguments.
	 * @param array $assoc_args Command associative arguments.
	 *
	 * @subcommand empty-posts
	 */
	public function empty_posts( $args, $assoc_args ) {
		global $wpdb;

		// Get parameters
		$post_type  = (string) WP_CLI\Utils\get_flag_value( $assoc_args, 'post_type', '' );
		$batch_size = (int) WP_CLI\Utils\get_flag_value( $assoc_args, 'batch-size', 100 );

		if ( empty( $post_type ) ) {
			WP_CLI::error( 'Please provide one or more post types separated by comma using --post_type parameter.' );
		}

		// Get total post count directly from database for reliability
		$post_types = explode( ',', $post_type );
		$placeholders = implode( ', ', array_fill( 0, count( $post_types ), '%s' ) );

		$query = $wpdb->prepare(
			"SELECT COUNT(*) FROM $wpdb->posts WHERE post_type IN ($placeholders)",
			$post_types
		);

		$total_count = (int) $wpdb->get_var( $query );

		if ( 0 === $total_count ) {
			WP_CLI::warning( 'No posts found for the specified post type(s).' );
			return;
		}

		// Confirm deletion
		WP_CLI::confirm(
			sprintf( 'You are about to delete all %d posts of type(s) "%s". Continue?', $total_count, $post_type ),
			$assoc_args
		);

		// Set up progress display
		$progress = \WP_CLI\Utils\make_progress_bar( 'Deleting posts', $total_count );
		$deleted_count = 0;

		// Start bulk operation
		self::start_bulk_operation();

		// Process in batches
		$remaining = $total_count;

		while ( $remaining > 0 ) {
			// Get post IDs directly from database
			$post_ids_query = $wpdb->prepare(
				"SELECT ID FROM $wpdb->posts WHERE post_type IN ($placeholders) LIMIT %d",
				array_merge( $post_types, array( $batch_size ) )
			);

			$post_ids = $wpdb->get_col( $post_ids_query );

			if ( empty( $post_ids ) ) {
				break; // No more posts found
			}

			$current_batch_size = count( $post_ids );

			// Delete posts in this batch
			foreach ( $post_ids as $post_id ) {
				wp_delete_post( $post_id, true ); // true = force delete, bypass trash
				$deleted_count++;
				$progress->tick();
			}

			$remaining -= $current_batch_size;

			// Flush the cache periodically to avoid memory issues
			if ( $deleted_count % 500 === 0 ) {
				wp_cache_flush();
			}
		}

		// End bulk operation
		self::end_bulk_operation();
		$progress->finish();

		// Report success
		WP_CLI::success( sprintf( 'Successfully deleted %d posts.', $deleted_count ) );
	}

	/**
	 * Deletes all the terms of one or more given taxonomies.
	 *
	 * ## OPTIONS
	 *
	 * [--taxonomy=<taxonomies>]
	 * : List of taxonomies to delete separated by comma.
	 *
	 * [--batch-size=<batch-size>]
	 * : Number of terms to delete in a single batch. Defaults to 100.
	 *
	 * [--yes]
	 * : Skip confirmation prompt.
	 *
	 * ## EXAMPLES
	 *
	 *     # Delete all terms of taxonomy 'category'
	 *     $ wp etl janitor empty-terms --taxonomy=category
	 *
	 *     # Delete all terms of taxonomies 'category' and 'post_tag' without confirmation
	 *     $ wp etl janitor empty-terms --taxonomy=category,post_tag --yes
	 *
	 *     # Delete terms in larger batches (500 at a time)
	 *     $ wp etl janitor empty-terms --taxonomy=category --batch-size=500
	 *
	 * @param array $args       Command arguments.
	 * @param array $assoc_args Command associative arguments.
	 *
	 * @subcommand empty-terms
	 */
	public function empty_terms( $args, $assoc_args ) {
		global $wpdb;

		// Get parameters
		$taxonomy = (string) WP_CLI\Utils\get_flag_value( $assoc_args, 'taxonomy', '' );
		$batch_size = (int) WP_CLI\Utils\get_flag_value( $assoc_args, 'batch-size', 100 );

		if ( empty( $taxonomy ) ) {
			WP_CLI::error( 'Please provide one or more taxonomies separated by comma using --taxonomy parameter.' );
		}

		// Get taxonomies array
		$taxonomies = explode( ',', $taxonomy );
		$placeholders = implode( ', ', array_fill( 0, count( $taxonomies ), '%s' ) );

		// Get total term count directly from database for reliability
		$query = $wpdb->prepare(
			"SELECT COUNT(*) FROM $wpdb->term_taxonomy WHERE taxonomy IN ($placeholders)",
			$taxonomies
		);

		$total_count = (int) $wpdb->get_var( $query );

		if ( 0 === $total_count ) {
			WP_CLI::warning( 'No terms found for the specified taxonomy/taxonomies.' );
			return;
		}

		// Confirm deletion
		WP_CLI::confirm(
			sprintf( 'You are about to delete all %d terms from taxonomy/taxonomies "%s". Continue?', $total_count, $taxonomy ),
			$assoc_args
		);

		// Set up progress display
		$progress = \WP_CLI\Utils\make_progress_bar( 'Deleting terms', $total_count );
		$deleted_count = 0;

		// Start bulk operation
		self::start_bulk_operation();

		// Process in batches
		$remaining = $total_count;

		while ( $remaining > 0 ) {
			// Get term IDs directly from database
			$term_ids_query = $wpdb->prepare(
				"SELECT t.term_id
				FROM $wpdb->terms AS t
				INNER JOIN $wpdb->term_taxonomy AS tt ON t.term_id = tt.term_id
				WHERE tt.taxonomy IN ($placeholders)
				LIMIT %d",
				array_merge( $taxonomies, array( $batch_size ) )
			);

			$term_ids = $wpdb->get_col( $term_ids_query );

			if ( empty( $term_ids ) ) {
				break; // No more terms found
			}

			$current_batch_size = count( $term_ids );

			// Delete terms in this batch
			foreach ( $term_ids as $term_id ) {
				foreach ( $taxonomies as $tax ) {
					wp_delete_term( $term_id, $tax );
				}
				$deleted_count++;
				$progress->tick();
			}

			$remaining -= $current_batch_size;

			// Flush the cache periodically to avoid memory issues
			if ( $deleted_count % 500 === 0 ) {
				wp_cache_flush();
			}
		}

		// End bulk operation
		self::end_bulk_operation();
		$progress->finish();

		// Report success
		WP_CLI::success( sprintf( 'Successfully deleted %d terms.', $deleted_count ) );
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
