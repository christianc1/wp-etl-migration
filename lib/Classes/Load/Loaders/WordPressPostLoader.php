<?php
/**
 * Class: WordPress Post Loader
 *
 * Loads posts into WordPress from ETL rows.
 *
 * @package TenupETL\Classes\Load\Loaders
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\{ WithLogging, WithSideLoadMedia };

use Flow\ETL\{FlowContext, Loader, Rows, Row};
use function Flow\ETL\DSL\{integer_entry, rows_to_array, string_entry};
use TenupETL\Classes\Config\GlobalConfig;
use Flow\ETL\Adapter\WordPress\Loaders\{WPPostsLoader, WPPostMetaLoader};

/**
 * Class WordPressPostLoader
 *
 * Handles loading posts into WordPress during ETL process.
 */
class WordPressPostLoader extends BaseLoader implements Loader, RowMutator {
	use WithLogging;
	use WithSideLoadMedia;
	use WithRowMutation;

	/**
	 * The adapter to use for loading posts
	 *
	 * @var WPPostsLoader
	 */
	protected $adapter;

	/**
	 * Keep track of the last normalized data for garbage collection
	 *
	 * @var array|null
	 */
	private $last_normalized_data = null;

	public function __construct(
		protected array $step_config,
		protected GlobalConfig $global_config,
	) {
		parent::__construct( $step_config, $global_config );

		$this->adapter = new WPPostsLoader( $step_config['args'] ?? [] );
		$this->adapter->withDateTimeFormat( \DateTimeInterface::ATOM );
	}

	/**
	 * Run the loader
	 *
	 * @param object $state The ETL state object.
	 * @return void
	 */
	public function run( $state ) {
		$state
			->write(
				$this
			);

		$this->log( 'Loading posts with WordPress core apis.', 'progress' );
	}

	/**
	 * Load rows into WordPress as posts
	 *
	 * @param Rows        $rows    The rows to load.
	 * @param FlowContext $context The flow context.
	 * @return void
	 */
	public function load( Rows $rows, FlowContext $context ): void {
		$normalizer = $this->adapter->create_normalizer( $context );
		$processed_count = 0;
		$memory_cleanup_interval = 10; // Clean up memory every X posts

		foreach ( $rows as $row ) {
			$row = $this->remove_skip_fields( $row );

			try {
				if ( isset( $this->step_config['upsert'] ) && $this->step_config['upsert'] ) {
					// Check for existing post
					$existing_post = $this->post_exists( $row );
					if ( $existing_post ) {
						$this->log( 'Updating post: ' . $row->valueOf( 'post.post_title' ), 'progress' );

						$row = $this->mutate_row( $row->add( integer_entry( 'post.ID', $existing_post ) ) );
					}
				}

				$post_id = $this->adapter->insertPost( $row, normalizer: $normalizer );
			} catch ( \Exception $e ) {
				$this->log( 'Error inserting post: ' . $row->valueOf( 'post.post_title' ), 'warning' );
				$this->log( $e->getMessage(), 'warning' );
				continue;
			}

			// Add post_id to the row
			if ( ! $row->has( 'post.ID' ) ) {
				$row = $this->mutate_row( $row->add( integer_entry( 'post.ID', $post_id ) ) );
			}

			// Handle thumbnail
			if ( isset( $post_meta['_remote_featured_media'] ) && $post_meta['_remote_featured_media'] ) {
				$attachment_id = $this->sideload_media( $post_meta['_remote_featured_media'], $post_id );

				if ( ! is_wp_error( $attachment_id ) ) {
					set_post_thumbnail( $post_id, $attachment_id );
					$row = $this->mutate_row( $row->add( integer_entry( 'post.featured_media', $attachment_id ) ) );
				}
			}
			$this->log( 'Loaded ' . $row->valueOf( 'post.post_title' ) . ' as WP_Post ' . $post_id, 'progress' );
		}
	}

	/**
	 * Remove skip fields from the row
	 *
	 * @param Row $row The row to remove skip fields from.
	 * @return Row The row with skip fields removed.
	 */
	protected function remove_skip_fields( Row $row ): Row {
		if ( ! isset( $this->step_config['skip_fields'] ) || empty( $this->step_config['skip_fields'] ) ) {
			return $row;
		}

		$fields = $this->step_config['skip_fields'];
		$fields = array_merge(
			$fields,
			array_map( fn( $field ) => 'post.' . $field, $fields )
		);

		foreach ( $fields as $field ) {
			if ( $row->has( $field ) ) {
				$row = $row->remove( $field );
			}
		}

		// Post title and post content are required fields.
		$post = get_post( $this->post_exists( $row ) );
		if ( in_array( 'post.post_title', $fields ) ) {

			if ( $post ) {
				$row = $row->add( string_entry( 'post.post_title', $post->post_title ) );
			} else {
				$row = $row->add( string_entry( 'post.post_title', 'New ' . $this->valueOf( 'post.post_type' ) ) );
			}
		}

		if ( in_array( 'post.post_content', $fields ) ) {
			if ( $post ) {
				$row = $row->add( string_entry( 'post.post_content', $post->post_content ) );
			} else {
				$row = $row->add( string_entry( 'post.post_content', 'Empty content for ' . $this->valueOf( 'post.post_type' ) ) );
			}
		}

		return $row;
	}

	/**
	 * Check if the post exists
	 *
	 * @param Row $row The row to check if the post exists.
	 * @return int The post ID if the post exists, 0 otherwise.
	 */
	private function post_exists( Row $row ): int {
		$post_id = $row->has( 'post.ID' ) ? $row->valueOf( 'post.ID' ) : null;
		if ( $post_id ) {
			return $post_id;
		}

		$post_name = $row->has( 'post.post_name' ) ? $row->valueOf( 'post.post_name' ) : null;
		if ( $post_name ) {
			$args = [
				'name' => $post_name,
				'post_type' => $row->valueOf( 'post.post_type' ),
				'posts_per_page' => 1,
			];

			$query = get_posts( $args );

			if ( $query ) {
				return $query[0]->ID;
			}
		}

		return 0;
	}
}
