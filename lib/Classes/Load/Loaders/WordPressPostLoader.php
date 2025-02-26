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

use Flow\ETL\{FlowContext, Loader, Rows};
use function Flow\ETL\DSL\{integer_entry, rows_to_array};
use TenupETL\Classes\Config\GlobalConfig;
use Flow\ETL\Adapter\WordPress\{WPPostsLoader, WPPostMetaLoader};

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

	public function __construct(
		protected array $step_config,
		protected GlobalConfig $global_config,
	) {
		parent::__construct( $step_config, $global_config );

		$this->posts_adapter = new WPPostsLoader( $step_config['args'] ?? [] );
		$this->posts_adapter->withDateTimeFormat( 'Y-m-d H:i:s' );

		$this->meta_adapter = new WPPostMetaLoader( $step_config['args'] ?? [] );
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
		$normalizer = $this->posts_adapter->create_normalizer( $context );

		foreach ( $rows as $row ) {
			try {
				$post_id = $this->posts_adapter->insertPost( $row, normalizer: $normalizer );
			} catch ( \Exception $e ) {
				$this->log( 'Error inserting post: ' . $row->valueOf( 'post.post_title' ), 'warning' );
				$this->log( $e->getMessage(), 'warning' );
				continue;
			}

			// Add post_id to the row
			$row = $this->mutate_row( $row->add( integer_entry( 'post.ID', $post_id ) ) );

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
}
