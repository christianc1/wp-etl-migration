<?php
/**
 * Class: WordPress Post Transformation Pipeline
 *
 * Handles transforming data into WordPress posts with associated metadata.
 *
 * @package TenupETL\Classes\Transform\TransformationPipelines
 */

namespace TenupETL\Classes\Transform\TransformationPipelines;

use Flow\ETL\DataFrame;
use Flow\ETL\Row;

use function TenupETL\Classes\Transform\Transformers\{simple_transformer};
use function Flow\ETL\DSL\{ref, cast, equal, when, lit, entry, now };

/**
 * Pipeline for transforming data into WordPress posts.
 *
 * Provides methods for setting post data like title, content, status etc.
 * and handles post meta and taxonomy relationships.
 */
class WordPressPostPipeline extends BaseTransformationPipeline {

	/**
	 * The post type to create.
	 *
	 * @var string
	 */
	public $post_type = 'post';

	/**
	 * Default values for post fields.
	 *
	 * @var array
	 */
	public $defaults = [
		'post_title'   => 'Default post title',
		'post_name'    => 'default-post-name',
		'post_status'  => 'draft',
		'post_content' => 'Default post content',
		'post_author'  => 1,
	];

	/**
	 * Prefix for core post fields.
	 *
	 * @var string
	 */
	protected $core_prefix = 'post';

	/**
	 * Prefix for post meta fields.
	 *
	 * @var string
	 */
	protected $meta_prefix = 'meta';

	/**
	 * Prefix for taxonomy fields.
	 *
	 * @var string
	 */
	protected $tax_prefix = 'tax';

	/**
	 * Prefix for ledger fields.
	 *
	 * @var string
	 */
	protected $ledger_prefix = 'ledger';

	/**
	 * Get prefixed core field name.
	 *
	 * @param string $column The core field name.
	 * @return string The prefixed field name.
	 */
	public function core( string $column ) {
		return $this->core_prefix . '.' . $column;
	}

	/**
	 * Get prefixed meta field name.
	 *
	 * @param string $meta_key The meta key.
	 * @return string The prefixed meta field name.
	 */
	public function meta( string $meta_key ) {
		return $this->meta_prefix . '.' . $meta_key;
	}

	/**
	 * Get prefixed taxonomy field name.
	 *
	 * @param string $tax_name The taxonomy name.
	 * @return string The prefixed taxonomy field name.
	 */
	public function tax( string $tax_name ) {
		return $this->tax_prefix . '.' . $tax_name;
	}

	/**
	 * Get prefixed ledger field name.
	 *
	 * @param string $column The ledger field name.
	 * @return string The prefixed ledger field name.
	 */
	public function ledger( string $column ) {
		return $this->ledger_prefix . '.' . $column;
	}

	/**
	 * Run the transformation pipeline.
	 *
	 * Sets up all post data fields in sequence.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	public function run(): TransformationPipeline {
		$this
			->prepare()
			->post_type()
			->post_title()
			->post_name()
			->post_author()
			->post_status()
			->post_content()
			->post_excerpt()
			->post_date()
			->post_modified()
			->etl_post_meta()
			->post_meta()
			->ledger_records()
			->tax_terms();

		return $this;
	}

	/**
	 * Get default value for a field.
	 *
	 * @param string $key The field key.
	 * @return mixed The default value wrapped in lit().
	 */
	protected function get_default( string $key ): mixed {
		return $this->defaults[ $key ] ? lit( $this->defaults[ $key ] ) : lit( '' );
	}

	/**
	 * Set the post type.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_type(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_type' ), lit( $this->post_type ) );

		return $this;
	}

	/**
	 * Set the post author.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_author(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_author' ), $this->get_default( 'post_author' ) );

		return $this;
	}

	/**
	 * Set the post title.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_title(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_title' ), lit( $this->get_default( 'post_title' ) ) );

		return $this;
	}

	/**
	 * Set the post slug.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_name(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_name' ), lit( $this->get_default( 'post_name' ) ) );

		return $this;
	}

	/**
	 * Set the post content.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_content(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_content' ), $this->get_default( 'post_content' ) );

		return $this;
	}

	/**
	 * Set the post excerpt.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_excerpt(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_excerpt' ), lit( '' ) );

		return $this;
	}

	/**
	 * Set the post status.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_status(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_status' ), lit( $this->get_default( 'post_status' ) ) );

		return $this;
	}

	/**
	 * Set the post date.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_date(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_date' ), now() );

		return $this;
	}

	/**
	 * Set the post modified date.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_modified(): TransformationPipeline {
		$this->state
			->withEntry( $this->core( 'post_modified_date' ), now() );

		return $this;
	}

	/**
	 * Set internal post meta.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function etl_post_meta(): TransformationPipeline {
		$this->state
			->withEntry(
				$this->meta( 'etl_pipeline' ),
				lit( static::class )
			);

		return $this;
	}

	/**
	 * Set custom post meta.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function post_meta(): TransformationPipeline {
		return $this;
	}

	/**
	 * Set tax terms.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function tax_terms(): TransformationPipeline {
		return $this;
	}

	/**
	 * Set ledger records.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function ledger_records(): TransformationPipeline {
		return $this;
	}
}
