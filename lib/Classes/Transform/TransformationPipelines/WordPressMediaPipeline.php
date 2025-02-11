<?php
/**
 * Class: WordPress Media Transformation Pipeline
 *
 * Handles transforming media/attachment data during ETL process.
 * Extends WordPressPostPipeline to provide media-specific transformation functionality.
 *
 * @package TenupETL\Classes\Transform\TransformationPipelines
 */

namespace TenupETL\Classes\Transform\TransformationPipelines;

use Flow\ETL\DataFrame;
use Flow\ETL\Row;

use function TenupETL\Classes\Transform\Transformers\{simple_transformer};
use function Flow\ETL\DSL\{ref, cast, equal, when, lit, entry, now };

/**
 * Class WordPressMediaPipeline
 *
 * Handles transformation pipeline for WordPress media/attachment posts.
 */
class WordPressMediaPipeline extends WordPressPostPipeline {

	/**
	 * Post type for media items
	 *
	 * @var string
	 */
	public $post_type = 'attachment';

	/**
	 * Default values for media posts
	 *
	 * @var array
	 */
	public $defaults = [
		'post_title'   => 'Default post title',
		'post_name'    => 'default-post-name',
		'post_status'  => 'draft',
		'post_content' => 'Default post content',
	];

	/**
	 * Run the media transformation pipeline
	 *
	 * @return WordPressMediaPipeline Current pipeline instance
	 */
	public function run(): WordPressMediaPipeline {
		$this
			->prepare()
			->remote_url()
			->local_path()
			->media_meta();

		return $this;
	}

	/**
	 * Prepare the pipeline for media transformations
	 *
	 * @return WordPressMediaPipeline Current pipeline instance
	 */
	public function prepare(): WordPressMediaPipeline {
		return $this;
	}

	/**
	 * Set up remote URL entry for media items
	 *
	 * @return WordPressMediaPipeline Current pipeline instance
	 */
	protected function remote_url(): WordPressMediaPipeline {
		$this->state
			->withEntry( 'media.remote_url', lit( [] ) );

		return $this;
	}

	/**
	 * Set up local path entry for media items
	 *
	 * @return WordPressMediaPipeline Current pipeline instance
	 */
	protected function local_path(): WordPressMediaPipeline {
		$this->state
			->withEntry( 'wp.local_path', lit( '' ) );

		return $this;
	}

	/**
	 * Set up media meta information
	 *
	 * @return WordPressMediaPipeline Current pipeline instance
	 */
	protected function media_meta(): WordPressMediaPipeline {
		$this->state
			->withEntry(
				'wp.media_meta',
				simple_transformer(
					fn() => [
						'etl_pipeline' => static::class,
					]
				)
			);

		return $this;
	}
}
